// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/internal/controller/util"
)

// DRPolicyReconciler reconciles a DRPolicy object
type DRPolicyReconciler struct {
	client.Client
	APIReader         client.Reader
	Log               logr.Logger
	Scheme            *runtime.Scheme
	MCVGetter         util.ManagedClusterViewGetter
	ObjectStoreGetter ObjectStoreGetter
	RateLimiter       *workqueue.TypedRateLimiter[reconcile.Request]
}

// ReasonValidationFailed is set when the DRPolicy could not be validated or is not valid
const ReasonValidationFailed = "ValidationFailed"

// ReasonDRClusterNotFound is set when the DRPolicy could not find the referenced DRCluster(s)
const ReasonDRClusterNotFound = "DRClusterNotFound"

// ReasonDRClustersUnavailable is set when the DRPolicy has none of the referenced DRCluster(s) are in a validated state
const ReasonDRClustersUnavailable = "DRClustersUnavailable"

// AllDRPolicyAnnotation is added to related resources that can be watched to reconcile all related DRPolicy resources
const AllDRPolicyAnnotation = "drpolicy.ramendr.openshift.io"

//nolint:lll
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=drpolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=drpolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ramendr.openshift.io,resources=drpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=work.open-cluster-management.io,resources=manifestworks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="policy.open-cluster-management.io",resources=placementbindings,verbs=list;watch
// +kubebuilder:rbac:groups="policy.open-cluster-management.io",resources=policies,verbs=list;watch
// +kubebuilder:rbac:groups="",namespace=system,resources=secrets,verbs=get;update
// +kubebuilder:rbac:groups="policy.open-cluster-management.io",namespace=system,resources=placementbindings,verbs=get;create;update;delete
// +kubebuilder:rbac:groups="policy.open-cluster-management.io",namespace=system,resources=policies,verbs=get;create;update;delete
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=view.open-cluster-management.io,resources=managedclusterviews,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DRPolicy object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
//
//nolint:cyclop,funlen
func (r *DRPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("drp", req.NamespacedName.Name, "rid", util.GetRID())
	log.Info("reconcile enter")

	defer log.Info("reconcile exit")

	drpolicy := &ramen.DRPolicy{}
	if err := r.Client.Get(ctx, req.NamespacedName, drpolicy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(fmt.Errorf("get: %w", err))
	}

	u := &drpolicyUpdater{ctx, drpolicy, r.Client, log}

	_, ramenConfig, err := ConfigMapGet(ctx, r.APIReader)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("config map get: %w", u.validatedSetFalse("ConfigMapGetFailed", err))
	}

	if err := util.CreateRamenOpsNamespace(ctx, r.Client, ramenConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create RamenOpsNamespace: %w",
			u.validatedSetFalse("NamespaceCreateFailed", err))
	}

	drclusters, drClusterIDsToNames, err := r.getDRClusterDetails(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("drclusters details: %w", u.validatedSetFalse("drClusterDetailsFailed", err))
	}

	secretsUtil := &util.SecretsUtil{Client: r.Client, APIReader: r.APIReader, Ctx: ctx, Log: log}
	// DRPolicy is marked for deletion
	if util.ResourceIsDeleted(drpolicy) &&
		controllerutil.ContainsFinalizer(drpolicy, drPolicyFinalizerName) {
		return ctrl.Result{}, u.deleteDRPolicy(drclusters, secretsUtil, ramenConfig)
	}

	log.Info("create/update")

	reason, err := validateDRPolicy(drpolicy, drclusters)
	if err != nil {
		statusErr := u.validatedSetFalse(reason, err)
		if !errors.Is(statusErr, err) || reason != ReasonDRClusterNotFound {
			return ctrl.Result{}, fmt.Errorf("validate: %w", statusErr)
		}

		log.Error(err, "Missing dependent resources")

		// will be reconciled later based on DRCluster watch events
		return ctrl.Result{}, nil
	}

	if err := u.addLabelsAndFinalizers(); err != nil {
		return ctrl.Result{}, fmt.Errorf("finalizer add update: %w", u.validatedSetFalse("FinalizerAddFailed", err))
	}

	return r.reconcile(u, drclusters, secretsUtil, ramenConfig, drClusterIDsToNames)
}

//nolint:unparam
func (r *DRPolicyReconciler) reconcile(
	u *drpolicyUpdater,
	drclusters *ramen.DRClusterList,
	secretsUtil *util.SecretsUtil,
	ramenConfig *ramen.RamenConfig,
	drClusterIDsToNames map[string]string,
) (ctrl.Result, error) {
	if err := u.validatedSetTrue("Succeeded", "drpolicy validated"); err != nil {
		return ctrl.Result{}, fmt.Errorf("unable to set drpolicy validation: %w", err)
	}

	if err := updatePeerClasses(u, r.MCVGetter); err != nil {
		return ctrl.Result{}, fmt.Errorf("drpolicy peerClass update: %w", err)
	}

	if err := propagateS3Secret(u.object, drclusters, secretsUtil, ramenConfig, u.log); err != nil {
		return ctrl.Result{}, fmt.Errorf("drpolicy deploy: %w", err)
	}

	// we will be able to validate conflicts only after PeerClasses are updated
	err := validatePolicyConflicts(u.ctx, r.APIReader, u.object, drClusterIDsToNames)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("drpolicy conflict validate failed")
	}

	if err := r.initiateDRPolicyMetrics(u.object); err != nil {
		return ctrl.Result{}, fmt.Errorf("error in intiating policy metrics: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *DRPolicyReconciler) initiateDRPolicyMetrics(drpolicy *ramen.DRPolicy) error {
	isMetro, _, err := dRPolicySupportsMetro(drpolicy, nil)
	if err != nil {
		return fmt.Errorf("failed to check if DRPolicy supports Metro: %w", err)
	}

	// Do not set metric for metro-dr
	if !isMetro {
		if err := r.setDRPolicyMetrics(drpolicy); err != nil {
			return fmt.Errorf("error in setting drpolicy metrics: %w", err)
		}
	}

	return nil
}

func (r *DRPolicyReconciler) getDRClusterDetails(ctx context.Context) (*ramen.DRClusterList, map[string]string, error) {
	drClusters := &ramen.DRClusterList{}
	if err := r.Client.List(ctx, drClusters); err != nil {
		return nil, nil, fmt.Errorf("drclusters list: %w", err)
	}

	drClusterIDsToNames := map[string]string{}

	for idx := range drClusters.Items {
		mc, err := util.NewManagedClusterInstance(ctx, r.Client, drClusters.Items[idx].GetName())
		if err != nil {
			r.Log.Error(err, "Failed to get a new MC instance", "drcluster", drClusters.Items[idx].GetName())

			continue
		}

		clID, err := mc.ClusterID()
		if err != nil {
			return nil, nil, fmt.Errorf("drclusters cluster ID (%s): %w", drClusters.Items[idx].GetName(), err)
		}

		drClusterIDsToNames[clID] = drClusters.Items[idx].GetName()
	}

	if len(drClusterIDsToNames) == 0 {
		return nil, nil, fmt.Errorf("no DRClusters found")
	}

	return drClusters, drClusterIDsToNames, nil
}

func validateDRPolicy(drpolicy *ramen.DRPolicy,
	drclusters *ramen.DRClusterList,
) (string, error) {
	// TODO: Ensure DRClusters exist and are validated? Also ensure they are not in a deleted state!?
	// If new DRPolicy and clusters are deleted, then fail reconciliation?
	if len(drpolicy.Spec.DRClusters) == 0 {
		return ReasonValidationFailed, fmt.Errorf("missing DRClusters list in policy")
	}

	reason, err := ensureDRClustersAvailable(drpolicy, drclusters)
	if err != nil {
		return reason, err
	}

	return "", nil
}

func (r *DRPolicyReconciler) setDRPolicyMetrics(drPolicy *ramen.DRPolicy) error {
	r.Log.Info(fmt.Sprintf("Setting metric: (%v)", DRPolicySyncIntervalSeconds))

	syncIntervalMetricsLabels := DRPolicySyncIntervalMetricLabels(drPolicy)
	metric := NewDRPolicySyncIntervalMetrics(syncIntervalMetricsLabels)

	schedulingIntervalSeconds, err := util.GetSecondsFromSchedulingInterval(drPolicy)
	if err != nil {
		return fmt.Errorf("unable to convert scheduling interval to seconds: %w", err)
	}

	metric.DRPolicySyncInterval.Set(schedulingIntervalSeconds)

	return nil
}

func ensureDRClustersAvailable(drpolicy *ramen.DRPolicy, drclusters *ramen.DRClusterList) (string, error) {
	found := 0
	validated := 0

	for _, specCluster := range drpolicy.Spec.DRClusters {
		for _, cluster := range drclusters.Items {
			if cluster.Name == specCluster {
				found++

				condition := util.FindCondition(cluster.Status.Conditions, ramen.DRClusterValidated)
				if condition != nil && condition.Status == metav1.ConditionTrue {
					validated++
				}
			}
		}
	}

	if found != len(drpolicy.Spec.DRClusters) {
		return ReasonDRClusterNotFound, fmt.Errorf("failed to find DRClusters specified in policy (%v)",
			drpolicy.Spec.DRClusters)
	}

	if validated == 0 {
		return ReasonDRClustersUnavailable, fmt.Errorf("none of the DRClusters are validated (%v)",
			drpolicy.Spec.DRClusters)
	}

	return "", nil
}

func validatePolicyConflicts(ctx context.Context,
	apiReader client.Reader,
	drpolicy *ramen.DRPolicy,
	drClusterIDsToNames map[string]string,
) error {
	// DRPolicy does not support both Sync and Async configurations in one single DRPolicy
	if len(drpolicy.Status.Sync.PeerClasses) > 0 && len(drpolicy.Status.Async.PeerClasses) > 0 {
		return fmt.Errorf("invalid DRPolicy: a policy cannot contain both sync and async configurations")
	}

	drpolicies, err := util.GetAllDRPolicies(ctx, apiReader)
	if err != nil {
		return fmt.Errorf("validate managed cluster in drpolicy %v failed: %w", drpolicy.Name, err)
	}

	err = hasConflictingDRPolicy(drpolicy, drpolicies, drClusterIDsToNames)
	if err != nil {
		return fmt.Errorf("validate managed cluster in drpolicy failed: %w", err)
	}

	return nil
}

// If two drpolicies have common managed cluster(s) and at least one of them is
// a metro supported drpolicy, then fail.
func hasConflictingDRPolicy(
	match *ramen.DRPolicy,
	list ramen.DRPolicyList,
	drClusterIDsToNames map[string]string,
) error {
	// Valid cases
	// [e1,w1] [e1,c1]
	// [e1,w1] [e1,w1]
	// [e1,w1] [e2,e3,w1]
	// [e1,e2,w1] [e3,e4,w1]
	// [e1,e2,w1,w2,c1] [e3,e4,w3,w4,c1]
	//
	// Failure cases
	// [e1,e2] [e1,e3] intersection e1, east=e1,e2 east=e1,e3
	// [e1,e2] [e1,w1]
	// [e1,e2,w1] [e1,e2,w1]
	// [e1,e2,c1] [e1,w1]
	for i := range list.Items {
		drp := &list.Items[i]

		if drp.ObjectMeta.Name == match.ObjectMeta.Name {
			continue
		}

		// None of the common managed clusters should belong to Metro clusters in either of the drpolicies.
		if haveOverlappingMetroZones(match, drp, drClusterIDsToNames) {
			return fmt.Errorf("drpolicy: %v has overlapping clusters with another drpolicy %v", match.Name, drp.Name)
		}
	}

	return nil
}

//nolint:errcheck
func haveOverlappingMetroZones(
	d1, d2 *ramen.DRPolicy,
	drClusterIDsToNames map[string]string,
) bool {
	d1ClusterNames := sets.NewString(util.DRPolicyClusterNames(d1)...)
	d1SupportsMetro, d1MetroClusters, _ := dRPolicySupportsMetro(d1, drClusterIDsToNames)
	d2ClusterNames := sets.NewString(util.DRPolicyClusterNames(d2)...)
	d2SupportsMetro, d2MetroClusters, _ := dRPolicySupportsMetro(d2, drClusterIDsToNames)
	commonClusters := d1ClusterNames.Intersection(d2ClusterNames)

	// No common managed clusters, so we are good
	if commonClusters.Len() == 0 {
		return false
	}

	// Lets check if the metro clusters in DRPolicy d2 belong to common managed clusters list
	if d2SupportsMetro {
		for _, v := range d2MetroClusters {
			if sets.NewString(v...).HasAny(commonClusters.List()...) {
				return true
			}
		}
	}

	// Lets check if the metro clusters in DRPolicy d1 belong to common managed clusters list
	if d1SupportsMetro {
		for _, v := range d1MetroClusters {
			if sets.NewString(v...).HasAny(commonClusters.List()...) {
				return true
			}
		}
	}

	return false
}

type drpolicyUpdater struct {
	ctx    context.Context
	object *ramen.DRPolicy
	client client.Client
	log    logr.Logger
}

func (u *drpolicyUpdater) deleteDRPolicy(drclusters *ramen.DRClusterList,
	secretsUtil *util.SecretsUtil,
	ramenConfig *ramen.RamenConfig,
) error {
	u.log.Info("delete")

	drpcs := ramen.DRPlacementControlList{}
	if err := secretsUtil.Client.List(secretsUtil.Ctx, &drpcs); err != nil {
		return fmt.Errorf("drpcs list: %w", err)
	}

	for i := range drpcs.Items {
		drpc1 := &drpcs.Items[i]
		if u.object.ObjectMeta.Name == drpc1.Spec.DRPolicyRef.Name {
			return fmt.Errorf("this drpolicy is referenced in existing drpc resource name '%v' ", drpc1.Name)
		}
	}

	if err := drPolicyUndeploy(u.object, drclusters, secretsUtil, ramenConfig, u.log); err != nil {
		return fmt.Errorf("drpolicy undeploy: %w", err)
	}

	if err := u.finalizerRemove(); err != nil {
		return fmt.Errorf("finalizer remove update: %w", err)
	}

	// proceed to delete metrics if non-metro-dr
	isMetro, _, err := dRPolicySupportsMetro(u.object,
		nil)
	if err != nil {
		return fmt.Errorf("failed to check if DRPolicy supports Metro: %w", err)
	}

	if !isMetro {
		// delete metrics if matching labels are found
		metricLabels := DRPolicySyncIntervalMetricLabels(u.object)
		DeleteDRPolicySyncIntervalMetrics(metricLabels)
	}

	return nil
}

func (u *drpolicyUpdater) validatedSetTrue(reason, message string) error {
	return u.statusConditionSet(ramen.DRPolicyValidated, metav1.ConditionTrue, reason, message)
}

func (u *drpolicyUpdater) validatedSetFalse(reason string, err error) error {
	if err1 := u.statusConditionSet(ramen.DRPolicyValidated, metav1.ConditionFalse, reason, err.Error()); err1 != nil {
		return err1
	}

	return err
}

func (u *drpolicyUpdater) statusConditionSet(conditionType string,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	conditions := &u.object.Status.Conditions

	if util.GenericStatusConditionSet(u.object, conditions, conditionType,
		status, reason, message, u.log) {
		return u.statusUpdate()
	}

	return nil
}

func (u *drpolicyUpdater) statusUpdate() error {
	return u.client.Status().Update(u.ctx, u.object)
}

const drPolicyFinalizerName = "drpolicies.ramendr.openshift.io/ramen"

func (u *drpolicyUpdater) addLabelsAndFinalizers() error {
	return util.NewResourceUpdater(u.object).
		AddLabel(util.OCMBackupLabelKey, util.OCMBackupLabelValue).
		AddFinalizer(drPolicyFinalizerName).
		Update(u.ctx, u.client)
}

func (u *drpolicyUpdater) finalizerRemove() error {
	return util.NewResourceUpdater(u.object).
		RemoveFinalizer(drPolicyFinalizerName).
		Update(u.ctx, u.client)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DRPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controller := ctrl.NewControllerManagedBy(mgr)
	if r.RateLimiter != nil {
		controller.WithOptions(ctrlcontroller.Options{
			RateLimiter: *r.RateLimiter,
		})
	}

	return controller.
		For(&ramen.DRPolicy{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.configMapMapFunc),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.secretMapFunc),
			builder.WithPredicates(util.CreateOrDeleteOrResourceVersionUpdatePredicate{}),
		).
		Watches(
			&ramen.DRCluster{},
			handler.EnqueueRequestsFromMapFunc(r.objectNameAsClusterMapFunc),
			builder.WithPredicates(util.CreateOrDeleteOrResourceVersionUpdatePredicate{}),
		).
		Watches(
			&ocmv1.ManagedCluster{},
			handler.EnqueueRequestsFromMapFunc(r.objectNameAsClusterMapFunc),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&viewv1beta1.ManagedClusterView{},
			handler.EnqueueRequestsFromMapFunc(r.mcvMapFun),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{})).
		Complete(r)
}

func (r *DRPolicyReconciler) configMapMapFunc(ctx context.Context, configMap client.Object) []reconcile.Request {
	if configMap.GetName() != HubOperatorConfigMapName || configMap.GetNamespace() != RamenOperatorNamespace() {
		return []reconcile.Request{}
	}

	labelAdded := util.AddLabel(configMap, util.OCMBackupLabelKey, util.OCMBackupLabelValue)

	if labelAdded {
		if err := r.Update(context.TODO(), configMap); err != nil {
			r.Log.Error(err, "Failed to add OCM backup label to ramen-hub-operator-config map")

			return []reconcile.Request{}
		}
	}

	drpolicies := &ramen.DRPolicyList{}
	if err := r.Client.List(context.TODO(), drpolicies); err != nil {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, len(drpolicies.Items))
	for i, drpolicy := range drpolicies.Items {
		requests[i].Name = drpolicy.GetName()
	}

	return requests
}

func (r *DRPolicyReconciler) secretMapFunc(ctx context.Context, secret client.Object) []reconcile.Request {
	if secret.GetNamespace() != RamenOperatorNamespace() {
		return []reconcile.Request{}
	}

	drpolicies := &ramen.DRPolicyList{}
	if err := r.Client.List(context.TODO(), drpolicies); err != nil {
		return []reconcile.Request{}
	}

	// TODO: Add optimzation to only reconcile policies that refer to the changed secret
	requests := make([]reconcile.Request, len(drpolicies.Items))
	for i, drpolicy := range drpolicies.Items {
		requests[i].Name = drpolicy.GetName()
	}

	return requests
}

// objectNameAsClusterMapFunc returns a list of DRPolicies that contain the object.Name. A DRCluster or a
// ManagedCluster object can be passed in as the cluster to find the list of policies to reconcile
func (r *DRPolicyReconciler) objectNameAsClusterMapFunc(
	ctx context.Context, cluster client.Object,
) []reconcile.Request {
	return r.getDRPoliciesForCluster(cluster.GetName())
}

func (r *DRPolicyReconciler) mcvMapFun(ctx context.Context, obj client.Object) []reconcile.Request {
	mcv, ok := obj.(*viewv1beta1.ManagedClusterView)
	if !ok {
		return []reconcile.Request{}
	}

	if _, ok := mcv.Annotations[AllDRPolicyAnnotation]; !ok {
		return []ctrl.Request{}
	}

	return r.getDRPoliciesForCluster(obj.GetNamespace())
}

func (r *DRPolicyReconciler) getDRPoliciesForCluster(clusterName string) []reconcile.Request {
	drpolicies := &ramen.DRPolicyList{}
	if err := r.Client.List(context.TODO(), drpolicies); err != nil {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, 0)

	for idx := range drpolicies.Items {
		drpolicy := &drpolicies.Items[idx]
		if util.DrpolicyContainsDrcluster(drpolicy, clusterName) {
			add := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: drpolicy.GetName(),
				},
			}
			requests = append(requests, add)
		}
	}

	return requests
}
