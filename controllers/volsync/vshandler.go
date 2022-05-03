/*
Copyright 2021 The RamenDR authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volsync

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
)

const (
	ServiceExportKind    string = "ServiceExport"
	ServiceExportGroup   string = "multicluster.x-k8s.io"
	ServiceExportVersion string = "v1alpha1"

	VolumeSnapshotKind                     string = "VolumeSnapshot"
	VolumeSnapshotIsDefaultAnnotation      string = "snapshot.storage.kubernetes.io/is-default-class"
	VolumeSnapshotIsDefaultAnnotationValue string = "true"

	PodVolumePVCClaimIndexName string = "spec.volumes.persistentVolumeClaim.claimName"

	VRGOwnerLabel          string = "volumereplicationgroups-owner"
	FinalSyncTriggerString string = "vrg-final-sync"

	SchedulingIntervalMinLength int = 2
	CronSpecMaxDayOfMonth       int = 28

	VolSyncDoNotDeleteLabel    = "volsync.backube/do-not-delete" // TODO: point to volsync constant once it is available
	VolSyncDoNotDeleteLabelVal = "true"

	ACMAppSubDoNotDeleteLabel    = "do-not-delete" // See: https://issues.redhat.com/browse/ACM-1256
	ACMAppSubDoNotDeleteLabelVal = "true"
)

type VSHandler struct {
	ctx                         context.Context
	client                      client.Client
	log                         logr.Logger
	owner                       metav1.Object
	schedulingInterval          string
	volumeSnapshotClassSelector metav1.LabelSelector // volume snapshot classes to be filtered label selector
	volumeSnapshotClassList     *snapv1.VolumeSnapshotClassList
}

func NewVSHandler(ctx context.Context, client client.Client, log logr.Logger, owner metav1.Object,
	schedulingInterval string, volumeSnapshotClassSelector metav1.LabelSelector) *VSHandler {
	return &VSHandler{
		ctx:                         ctx,
		client:                      client,
		log:                         log,
		owner:                       owner,
		schedulingInterval:          schedulingInterval,
		volumeSnapshotClassSelector: volumeSnapshotClassSelector,
		volumeSnapshotClassList:     nil, // Do not initialize until we need it
	}
}

// VSHandler requires an index on pods to keep track of persistent volume claims mounted
func IndexFieldsForVSHandler(ctx context.Context, fieldIndexer client.FieldIndexer) error {
	return fieldIndexer.IndexField(ctx, &corev1.Pod{}, PodVolumePVCClaimIndexName, func(o client.Object) []string {
		var res []string
		for _, vol := range o.(*corev1.Pod).Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			// just return the raw field value -- the indexer will take care of dealing with namespaces for us
			res = append(res, vol.PersistentVolumeClaim.ClaimName)
		}

		return res
	})
}

// returns replication destination only if create/update is successful and the RD is considered available.
// Callers should assume getting a nil replication destination back means they should retry/requeue.
func (v *VSHandler) ReconcileRD(
	rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) (*volsyncv1alpha1.ReplicationDestination, error,
) {
	l := v.log.WithValues("rdSpec", rdSpec)

	if !rdSpec.ProtectedPVC.ProtectedByVolSync {
		return nil, fmt.Errorf("protectedPVC %s is not VolSync Enabled", rdSpec.ProtectedPVC.Name)
	}

	// Pre-allocated shared secret - DRPC will generate and propagate this secret from hub to clusters
	sshKeysSecretName := GetVolSyncSSHSecretNameFromVRGName(v.owner.GetName())
	// Need to confirm this secret exists on the cluster before proceeding, otherwise volsync will generate it
	secretExists, err := v.validateSecretAndAddVRGOwnerRef(sshKeysSecretName)
	if err != nil || !secretExists {
		return nil, err
	}

	// Check if a ReplicationSource is still here (Can happen if transitioning from primary to secondary)
	// Before creating a new RD for this PVC, make sure any ReplicationSource for this PVC is cleaned up first
	// This avoids a scenario where we create an RD that immediately syncs with an RS that still exists locally
	err = v.DeleteRS(rdSpec.ProtectedPVC.Name)
	if err != nil {
		return nil, err
	}

	var rd *volsyncv1alpha1.ReplicationDestination

	rd, err = v.createOrUpdateRD(rdSpec, sshKeysSecretName)
	if err != nil {
		return nil, err
	}

	err = v.reconcileServiceExportForRD(rd)
	if err != nil {
		return nil, err
	}

	if !rdStatusReady(rd, l) {
		return nil, nil
	}

	l.V(1).Info("ReplicationDestination Reconcile Complete")

	return rd, nil
}

// For ReplicationDestination - considered ready when a sync has completed
// - rsync address should be filled out in the status
// - latest image should be set properly in the status (at least one sync cycle has completed and we have a snapshot)
func rdStatusReady(rd *volsyncv1alpha1.ReplicationDestination, log logr.Logger) bool {
	if rd.Status == nil {
		return false
	}

	if rd.Status.Rsync == nil || rd.Status.Rsync.Address == nil {
		log.V(1).Info("ReplicationDestination waiting for Address ...")

		return false
	}

	// Additional check to make sure 1 sync has completed (i.e. latest image is set)
	if !isLatestImageReady(rd.Status.LatestImage) {
		log.V(1).Info("ReplicationDestination waiting for latest image to be set (sync complete) ...")

		return false
	}

	return true
}

func (v *VSHandler) createOrUpdateRD(
	rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
	sshKeysSecretName string) (*volsyncv1alpha1.ReplicationDestination, error,
) {
	l := v.log.WithValues("rdSpec", rdSpec)

	volumeSnapshotClassName, err := v.GetVolumeSnapshotClassFromPVCStorageClass(rdSpec.ProtectedPVC.StorageClassName)
	if err != nil {
		return nil, err
	}

	pvcAccessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce} // Default value
	if len(rdSpec.ProtectedPVC.AccessModes) > 0 {
		pvcAccessModes = rdSpec.ProtectedPVC.AccessModes
	}

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationDestinationName(rdSpec.ProtectedPVC.Name),
			Namespace: v.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rd, func() error {
		if err := ctrl.SetControllerReference(v.owner, rd, v.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")

			return fmt.Errorf("%w", err)
		}

		addVRGOwnerLabel(v.owner, rd)

		rd.Spec.Rsync = &volsyncv1alpha1.ReplicationDestinationRsyncSpec{
			ServiceType: v.getRsyncServiceType(),
			SSHKeys:     &sshKeysSecretName,

			ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
				CopyMethod:              volsyncv1alpha1.CopyMethodSnapshot,
				Capacity:                rdSpec.ProtectedPVC.Resources.Requests.Storage(),
				StorageClassName:        rdSpec.ProtectedPVC.StorageClassName,
				AccessModes:             pvcAccessModes,
				VolumeSnapshotClassName: &volumeSnapshotClassName,
			},
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	l.V(1).Info("ReplicationDestination createOrUpdate Complete", "op", op)

	return rd, nil
}

// Returns true only if runFinalSync is true and the final sync is done
// Returns replication source only if create/update is successful
// Callers should assume getting a nil replication source back means they should retry/requeue.
// Returns true/false if final sync is complete, and also returns an RS if one was reconciled.
//nolint:cyclop
func (v *VSHandler) ReconcileRS(rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec,
	runFinalSync bool) (bool /* finalSyncComplete */, *volsyncv1alpha1.ReplicationSource, error,
) {
	l := v.log.WithValues("rsSpec", rsSpec, "runFinalSync", runFinalSync)

	if !rsSpec.ProtectedPVC.ProtectedByVolSync {
		return false, nil, fmt.Errorf("protectedPVC %s is not VolSync Enabled", rsSpec.ProtectedPVC.Name)
	}

	// Pre-allocated shared secret - DRPC will generate and propagate this secret from hub to clusters
	sshKeysSecretName := GetVolSyncSSHSecretNameFromVRGName(v.owner.GetName())

	// Need to confirm this secret exists on the cluster before proceeding, otherwise volsync will generate it
	secretExists, err := v.validateSecretAndAddVRGOwnerRef(sshKeysSecretName)
	if err != nil || !secretExists {
		return false, nil, err
	}

	// Check if a ReplicationDestination is still here (Can happen if transitioning from secondary to primary)
	// Before creating a new RS for this PVC, make sure any ReplicationDestination for this PVC is cleaned up first
	// This avoids a scenario where we create an RS that immediately connects back to an RD that still exists locally
	// Need to be sure ReconcileRS is never called prior to restoring any PVC that need to be restored from RDs first
	err = v.DeleteRD(rsSpec.ProtectedPVC.Name)
	if err != nil {
		return false, nil, err
	}

	pvcOk, err := v.validatePVCBeforeFinalSync(rsSpec, runFinalSync)
	if !pvcOk || err != nil {
		return false, nil, err
	}

	replicationSource, err := v.createOrUpdateRS(rsSpec, sshKeysSecretName, runFinalSync)
	if err != nil {
		return false, nil, err
	}

	// Only return the RS if we've successfully completed a sync
	if !isRSLastSyncTimeReady(replicationSource.Status) {
		l.V(1).Info("ReplicationSource waiting for last sync time to be set (sync complete) ...")

		return false, nil, nil
	}

	//
	// For final sync only - check status to make sure the final sync is complete
	// and also run cleanup (removes PVC we just ran the final sync from)
	//
	if runFinalSync && isFinalSyncComplete(replicationSource, l) {
		return true, replicationSource, v.cleanupAfterRSFinalSync(rsSpec)
	}

	l.V(1).Info("ReplicationSource Reconcile Complete")

	return false, replicationSource, err
}

// Need to validate that our PVC is no longer in use (not mounted to a pod) before proceeding
// If in final sync and the source PVC no longer exists, this could be from
// a 2nd call to runFinalSync and we may have already cleaned up the PVC - so if pvc does not
// exist, treat the same as not in use - continue on with reconcile of the RS (and therefore
// check status to confirm final sync is complete)
func (v *VSHandler) validatePVCBeforeFinalSync(rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec,
	runFinalSync bool) (bool, error,
) {
	if !runFinalSync {
		return true, nil // No validation when not doing final sync, returning true
	}

	l := v.log.WithValues("rsSpec", rsSpec, "runFinalSync", runFinalSync)

	// If runFinalSync, check the PVC and make sure it's not in use before proceeding
	existsAndInUse, err := v.pvcExistsAndInUse(rsSpec.ProtectedPVC.Name)
	if err != nil {
		l.Error(err, "error checking if pvc is in use")

		return false, err
	}

	if existsAndInUse {
		l.Info("pvc is still in use, not reconciling RS for final sync yet ...")

		return false, nil
	}

	return true, nil // Good to proceed - PVC exists but is not in use or does not exist
}

func isFinalSyncComplete(replicationSource *volsyncv1alpha1.ReplicationSource, log logr.Logger) bool {
	if replicationSource.Status == nil || replicationSource.Status.LastManualSync != FinalSyncTriggerString {
		log.V(1).Info("ReplicationSource running final sync - waiting for status ...")

		return false
	}

	log.V(1).Info("ReplicationSource final sync complete")

	return true
}

func (v *VSHandler) cleanupAfterRSFinalSync(rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec) error {
	// Final sync is done, make sure PVC is cleaned up
	v.log.Info("Cleanup after final sync", "pvcName", rsSpec.ProtectedPVC.Name)

	return v.deletePVC(rsSpec.ProtectedPVC.Name)
}

// nolint: funlen
func (v *VSHandler) createOrUpdateRS(rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec,
	sshKeysSecretName string, runFinalSync bool) (*volsyncv1alpha1.ReplicationSource, error,
) {
	l := v.log.WithValues("rsSpec", rsSpec, "runFinalSync", runFinalSync)

	volumeSnapshotClassName, err := v.GetVolumeSnapshotClassFromPVCStorageClass(rsSpec.ProtectedPVC.StorageClassName)
	if err != nil {
		return nil, err
	}

	// Remote service address created for the ReplicationDestination on the secondary
	// The secondary namespace will be the same as primary namespace so use the vrg.Namespace
	remoteAddress := getRemoteServiceNameForRDFromPVCName(rsSpec.ProtectedPVC.Name, v.owner.GetNamespace())

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getReplicationSourceName(rsSpec.ProtectedPVC.Name),
			Namespace: v.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, rs, func() error {
		if err := ctrl.SetControllerReference(v.owner, rs, v.client.Scheme()); err != nil {
			l.Error(err, "unable to set controller reference")

			return fmt.Errorf("%w", err)
		}

		addVRGOwnerLabel(v.owner, rs)

		rs.Spec.SourcePVC = rsSpec.ProtectedPVC.Name

		if runFinalSync {
			l.V(1).Info("ReplicationSource - final sync")
			// Change the schedule to instead use a keyword trigger - to trigger
			// a final sync to happen
			rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Manual: FinalSyncTriggerString,
			}
		} else {
			// Set schedule
			scheduleCronSpec, err := v.getScheduleCronSpec()
			if err != nil {
				l.Error(err, "unable to parse schedulingInterval")

				return err
			}
			rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Schedule: scheduleCronSpec,
			}
		}

		rs.Spec.Rsync = &volsyncv1alpha1.ReplicationSourceRsyncSpec{
			SSHKeys: &sshKeysSecretName,
			Address: &remoteAddress,

			ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
				// Always using CopyMethod of snapshot for now - could use 'Clone' CopyMethod for specific
				// storage classes that support it in the future
				CopyMethod:              volsyncv1alpha1.CopyMethodSnapshot,
				VolumeSnapshotClassName: &volumeSnapshotClassName,
				// Not setting storageclassname - volsync can find that from the sourcePVC
			},
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	l.V(1).Info("ReplicationSource createOrUpdate Complete", "op", op)

	return rs, nil
}

// This doesn't need to specifically be in VSHandler - could be useful for non-volsync scenarios?
// Will look at annotations on the PVC, make sure the reconcile option from ACM is set to merge (or not exists)
// and then will remove ACM annotations and also add VRG as the owner.  This is to break the connection between
// the appsub and the PVC itself.  This way we can proceed to remove the app without the PVC being removed.
// We need the PVC left behind so we can fun a final sync on it (see ReconcileRS() with runFinalSync=true)
//
// Returns true if pvc preparation for final sync is complete
func (v *VSHandler) PreparePVCForFinalSync(pvcName string) (bool, error) {
	l := v.log.WithValues("pvcName", pvcName)

	// Confirm PVC exists and add our VRG as ownerRef
	pvc, err := v.validatePVCAndAddVRGOwnerRef(pvcName)
	if err != nil {
		l.Error(err, "unable to validate PVC or add ownership")

		return false, err
	}

	/*
	  TODO: the rest of this function is:
	  - Waiting for reconcile option to be set to "merge"
	  - Removing annotations on the PVC to "break" ACM's ownership so it will not delete the PVC
	    when the appsub is removed

	  These steps will not be required once: https://issues.redhat.com/browse/ACM-1256 is completed
	  (in ACM 2.6 timeframe) as the above step validatePVCAndAddVRGOwnerRef() will add the "do-not-delete"
	  label to the PVC which would then prevent ACM from cleaning up the PVC when the appsub is removed.

	  So this function can end here once the above enhancement is done
	*/

	// Check for annotation that indicates the PVC whether the ACM appsub will keep reconciling by re-taking
	// ownership of the PVC - if annotation is "mergeAndOwn" we need to wait for it to be removed or change to "merge"
	reconcileOption, ok := pvc.Annotations["apps.open-cluster-management.io/reconcile-option"]
	if ok && reconcileOption != "merge" {
		l.Info("pvc is still owned by appsub, need to wait", "pvc reconcile-option annotation", reconcileOption)

		return false, nil
	}

	// No annotation, or annotation is set to merge, we're good to go
	updatedAnnotations := map[string]string{}

	for currAnnotationKey, currAnnotationValue := range pvc.Annotations {
		// We want to only preserve annotations not from ACM (i.e. remove all ACM annotations to break ownership)
		if !strings.HasPrefix(currAnnotationKey, "apps.open-cluster-management.io") {
			updatedAnnotations[currAnnotationKey] = currAnnotationValue
		}
	}

	pvc.Annotations = updatedAnnotations

	err = v.client.Update(v.ctx, pvc)
	if err != nil {
		l.Error(err, "Error updating annotations on PVC to break appsub ownership")

		return false, fmt.Errorf("error updating annotations on PVC to break appsub ownership (%w)", err)
	}

	l.Info("pvc ready for final sync")

	return true, nil
}

// Will return true only if the pvc exists and in use - will not throw error if PVC not found
func (v *VSHandler) pvcExistsAndInUse(pvcName string) (bool, error) {
	_, err := v.getPVC(pvcName)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil // No error just indicate not exists and not in use
		}

		return false, err // error accessing the PVC, return it
	}

	v.log.V(1).Info("pvc found", "pvcName", pvcName)

	return v.isPvcInUse(pvcName)
}

func (v *VSHandler) isPvcInUse(pvcName string) (bool, error) {
	podUsingPVCList := &corev1.PodList{}

	err := v.client.List(context.Background(),
		podUsingPVCList, // Our custom index - needs to be setup in the cache (see IndexFieldsForVSHandler())
		client.MatchingFields{PodVolumePVCClaimIndexName: pvcName},
		client.InNamespace(v.owner.GetNamespace()))
	if err != nil {
		v.log.Error(err, "unable to lookup pods to see if they are using pvc", "pvcName", pvcName)

		return false, fmt.Errorf("unable to lookup pods to check if pvc is in use (%w)", err)
	}

	if len(podUsingPVCList.Items) == 0 {
		return false /* Not in use by any pod */, nil
	}

	inUsePodNames := []string{}
	for _, pod := range podUsingPVCList.Items {
		inUsePodNames = append(inUsePodNames, pod.GetName())
	}

	v.log.Info("pvc is in use by pod(s)", "pvcName", pvcName, "inUsePodNames", inUsePodNames)

	return true, nil
}

func (v *VSHandler) deletePVC(pvcName string) error {
	pvcToDelete := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: v.owner.GetNamespace(),
		},
	}

	err := v.client.Delete(v.ctx, pvcToDelete)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			v.log.Error(err, "error deleting pvc", "pvcName", pvcName)

			return fmt.Errorf("error deleting pvc (%w)", err)
		}
	} else {
		v.log.Info("deleted pvc", "pvcName", pvcName)
	}

	return nil
}

func (v *VSHandler) getPVC(pvcName string) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      pvcName,
			Namespace: v.owner.GetNamespace(),
		}, pvc)
	if err != nil {
		return pvc, fmt.Errorf("%w", err)
	}

	return pvc, nil
}

// Adds owner ref and ACM "do-not-delete" label to indicate that when the appsub is removed, ACM
// should not cleanup this PVC - we want it left behind so we can run a final sync
func (v *VSHandler) validatePVCAndAddVRGOwnerRef(pvcName string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := v.getPVC(pvcName)
	if err != nil {
		return nil, err
	}

	v.log.Info("PVC exists", "pvcName", pvcName)

	// Add Label to indicate that ACM should not delete/cleanup this pvc when the appsub is removed
	// and add VRG as owner
	err = v.addLabelAndVRGOwnerRefAndUpdate(pvc, ACMAppSubDoNotDeleteLabel, ACMAppSubDoNotDeleteLabelVal)
	if err != nil {
		return nil, err
	}

	v.log.V(1).Info("PVC validated", "pvc name", pvcName)

	return pvc, nil
}

func (v *VSHandler) validateSecretAndAddVRGOwnerRef(secretName string) (bool, error) {
	secret := &corev1.Secret{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      secretName,
			Namespace: v.owner.GetNamespace(),
		}, secret)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			v.log.Error(err, "Failed to get secret", "secretName", secretName)

			return false, fmt.Errorf("error getting secret (%w)", err)
		}

		// Secret is not found
		v.log.Info("Secret not found", "secretName", secretName)

		return false, nil
	}

	v.log.Info("Secret exists", "secretName", secretName)

	// Add VRG as owner
	if err := v.addOwnerReferenceAndUpdate(secret, v.owner); err != nil {
		v.log.Error(err, "Unable to update secret", "secretName", secretName)

		return true, err
	}

	v.log.V(1).Info("VolSync secret validated", "secret name", secretName)

	return true, nil
}

func (v *VSHandler) DeleteRS(pvcName string) error {
	// Remove a ReplicationSource by name that is owned (by parent vrg owner)
	currentRSListByOwner, err := v.listRSByOwner()
	if err != nil {
		return err
	}

	for i := range currentRSListByOwner.Items {
		rs := currentRSListByOwner.Items[i]

		if rs.GetName() == getReplicationSourceName(pvcName) {
			// Delete the ReplicationSource, log errors with cleanup but continue on
			if err := v.client.Delete(v.ctx, &rs); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationSource", "name", rs.GetName())
			} else {
				v.log.Info("Deleted ReplicationSource", "name", rs.GetName())
			}
		}
	}

	return nil
}

func (v *VSHandler) DeleteRD(pvcName string) error {
	// Remove a ReplicationDestination by name that is owned (by parent vrg owner)
	currentRDListByOwner, err := v.listRDByOwner()
	if err != nil {
		return err
	}

	for i := range currentRDListByOwner.Items {
		rd := currentRDListByOwner.Items[i]

		if rd.GetName() == getReplicationDestinationName(pvcName) {
			// Delete the ReplicationDestination, log errors with cleanup but continue on
			if err := v.client.Delete(v.ctx, &rd); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationDestination", "name", rd.GetName())
			} else {
				v.log.Info("Deleted ReplicationDestination", "name", rd.GetName())
			}
		}
	}

	return nil
}

func (v *VSHandler) CleanupRDNotInSpecList(rdSpecList []ramendrv1alpha1.VolSyncReplicationDestinationSpec) error {
	// Remove any ReplicationDestination owned (by parent vrg owner) that is not in the provided rdSpecList
	currentRDListByOwner, err := v.listRDByOwner()
	if err != nil {
		return err
	}

	for i := range currentRDListByOwner.Items {
		rd := currentRDListByOwner.Items[i]

		foundInSpecList := false

		for _, rdSpec := range rdSpecList {
			if rd.GetName() == getReplicationDestinationName(rdSpec.ProtectedPVC.Name) {
				foundInSpecList = true

				break
			}
		}

		if !foundInSpecList {
			// Delete the ReplicationDestination, log errors with cleanup but continue on
			if err := v.client.Delete(v.ctx, &rd); err != nil {
				v.log.Error(err, "Error cleaning up ReplicationDestination", "name", rd.GetName())
			} else {
				v.log.Info("Deleted ReplicationDestination", "name", rd.GetName())
			}
		}
	}

	return nil
}

// Make sure a ServiceExport exists to export the service for this RD to remote clusters
// See: https://access.redhat.com/documentation/en-us/red_hat_advanced_cluster_management_for_kubernetes/
// 2.4/html/services/services-overview#enable-service-discovery-submariner
func (v *VSHandler) reconcileServiceExportForRD(rd *volsyncv1alpha1.ReplicationDestination) error {
	// Using unstructured to avoid needing to require serviceexport in client scheme
	svcExport := &unstructured.Unstructured{}
	svcExport.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      getLocalServiceNameForRD(rd.GetName()), // Get name of the local service (this needs to be exported)
			"namespace": rd.GetNamespace(),
		},
	}
	svcExport.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   ServiceExportGroup,
		Kind:    ServiceExportKind,
		Version: ServiceExportVersion,
	})

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, svcExport, func() error {
		// Make this ServiceExport owned by the replication destination itself rather than the VRG
		// This way on relocate scenarios or failover/failback, when the RD is cleaned up the associated
		// ServiceExport will get cleaned up with it.
		if err := ctrlutil.SetOwnerReference(rd, svcExport, v.client.Scheme()); err != nil {
			v.log.Error(err, "unable to set controller reference", "resource", svcExport)

			return fmt.Errorf("%w", err)
		}

		return nil
	})

	v.log.V(1).Info("ServiceExport createOrUpdate Complete", "op", op)

	if err != nil {
		v.log.Error(err, "error creating or updating ServiceExport", "replication destination name", rd.GetName(),
			"namespace", rd.GetNamespace())

		return fmt.Errorf("error creating or updating ServiceExport (%w)", err)
	}

	v.log.V(1).Info("ServiceExport Reconcile Complete")

	return nil
}

func (v *VSHandler) listRSByOwner() (volsyncv1alpha1.ReplicationSourceList, error) {
	rsList := volsyncv1alpha1.ReplicationSourceList{}
	if err := v.listByOwner(&rsList); err != nil {
		v.log.Error(err, "Failed to list ReplicationSources for VRG", "vrg name", v.owner.GetName())

		return rsList, err
	}

	return rsList, nil
}

func (v *VSHandler) listRDByOwner() (volsyncv1alpha1.ReplicationDestinationList, error) {
	rdList := volsyncv1alpha1.ReplicationDestinationList{}
	if err := v.listByOwner(&rdList); err != nil {
		v.log.Error(err, "Failed to list ReplicationDestinations for VRG", "vrg name", v.owner.GetName())

		return rdList, err
	}

	return rdList, nil
}

// Lists only RS/RD with VRGOwnerLabel that matches the owner
func (v *VSHandler) listByOwner(list client.ObjectList) error {
	matchLabels := map[string]string{
		VRGOwnerLabel: v.owner.GetName(),
	}
	listOptions := []client.ListOption{
		client.InNamespace(v.owner.GetNamespace()),
		client.MatchingLabels(matchLabels),
	}

	if err := v.client.List(v.ctx, list, listOptions...); err != nil {
		v.log.Error(err, "Failed to list by label", "matchLabels", matchLabels)

		return fmt.Errorf("error listing by label (%w)", err)
	}

	return nil
}

func (v *VSHandler) EnsurePVCfromRD(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) error {
	l := v.log.WithValues("rdSpec", rdSpec)

	latestImage, err := v.getRDLatestImage(rdSpec.ProtectedPVC.Name)
	if err != nil {
		return err
	}

	if !isLatestImageReady(latestImage) {
		noSnapErr := fmt.Errorf("unable to find LatestImage from ReplicationDestination %s", rdSpec.ProtectedPVC.Name)
		l.Error(noSnapErr, "No latestImage")

		return noSnapErr
	}

	// Make copy of the ref and make sure API group is filled out correctly (shouldn't really need this part)
	vsImageRef := latestImage.DeepCopy()
	if vsImageRef.APIGroup == nil || *vsImageRef.APIGroup == "" {
		vsGroup := snapv1.GroupName
		vsImageRef.APIGroup = &vsGroup
	}

	l.V(1).Info("Latest Image for ReplicationDestination", "latestImage	", vsImageRef)

	return v.validateSnapshotAndEnsurePVC(rdSpec, *vsImageRef)
}

func (v *VSHandler) validateSnapshotAndEnsurePVC(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
	snapshotRef corev1.TypedLocalObjectReference) error {
	snap, err := v.validateSnapshotAndAddDoNotDeleteLabel(snapshotRef)
	if err != nil {
		return err
	}

	var pvc *corev1.PersistentVolumeClaim

	pvc, err = v.ensurePVCFromSnapshot(rdSpec, snapshotRef)
	if err != nil {
		return err
	}

	// Add ownerRef on snapshot pointing to the pvc - if/when the PVC gets cleaned up, then GC can cleanup the snap
	return v.addOwnerReferenceAndUpdate(snap, pvc)
}

//nolint:funlen,gocognit,cyclop
func (v *VSHandler) ensurePVCFromSnapshot(rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
	snapshotRef corev1.TypedLocalObjectReference) (*corev1.PersistentVolumeClaim, error) {
	l := v.log.WithValues("pvcName", rdSpec.ProtectedPVC.Name, "snapshotRef", snapshotRef)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdSpec.ProtectedPVC.Name,
			Namespace: v.owner.GetNamespace(),
		},
	}

	pvcNeedsRecreation := false

	op, err := ctrlutil.CreateOrUpdate(v.ctx, v.client, pvc, func() error {
		if !pvc.CreationTimestamp.IsZero() && !objectRefMatches(pvc.Spec.DataSource, &snapshotRef) {
			// If this pvc already exists and not pointing to our desired snapshot, we will need to
			// delete it and re-create as we cannot update the datasource
			pvcNeedsRecreation = true

			return nil
		}
		if pvc.Status.Phase == corev1.ClaimBound {
			// PVC already bound at this point
			l.V(1).Info("PVC already bound")

			return nil
		}

		if pvc.Labels == nil {
			pvc.Labels = rdSpec.ProtectedPVC.Labels
		} else {
			for key, val := range rdSpec.ProtectedPVC.Labels {
				pvc.Labels[key] = val
			}
		}

		accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce} // Default value
		if len(rdSpec.ProtectedPVC.AccessModes) > 0 {
			accessModes = rdSpec.ProtectedPVC.AccessModes
		}

		if pvc.CreationTimestamp.IsZero() { // set immutable fields
			pvc.Spec.AccessModes = accessModes
			pvc.Spec.StorageClassName = rdSpec.ProtectedPVC.StorageClassName

			// Only set when initially creating
			pvc.Spec.DataSource = &snapshotRef
		}

		pvc.Spec.Resources = rdSpec.ProtectedPVC.Resources

		return nil
	})
	if err != nil {
		l.Error(err, "Unable to createOrUpdate PVC from snapshot")

		return nil, fmt.Errorf("error creating or updating PVC from snapshot (%w)", err)
	}

	if pvcNeedsRecreation {
		needsRecreateErr := fmt.Errorf("pvc has incorrect datasource, will need to delete and recreate, pvc: %s",
			pvc.GetName())
		v.log.Error(needsRecreateErr, "Need to delete pvc (pvc restored from snapshot)")

		delErr := v.client.Delete(v.ctx, pvc)
		if delErr != nil {
			v.log.Error(delErr, "Error deleting pvc", "pvc name", pvc.GetName())
		}

		// Return error to indicate the ensurePVC should be attempted again
		return nil, needsRecreateErr
	}

	l.V(1).Info("PVC createOrUpdate Complete", "op", op)

	return pvc, nil
}

// Validates snapshot exists and adds VolSync "do-not-delete" label to indicate volsync should not cleanup this snapshot
func (v *VSHandler) validateSnapshotAndAddDoNotDeleteLabel(
	volumeSnapshotRef corev1.TypedLocalObjectReference) (*snapv1.VolumeSnapshot, error) {
	// Using unstructured to avoid needing to require VolumeSnapshot in client scheme
	volSnap := &snapv1.VolumeSnapshot{}

	err := v.client.Get(v.ctx, types.NamespacedName{
		Name:      volumeSnapshotRef.Name,
		Namespace: v.owner.GetNamespace(),
	}, volSnap)
	if err != nil {
		v.log.Error(err, "Unable to get VolumeSnapshot", "volumeSnapshotRef", volumeSnapshotRef)

		return nil, fmt.Errorf("error getting volumesnapshot (%w)", err)
	}

	// Add label to indicate that VolSync should not delete/cleanup this snapshot
	labelsUpdated := v.addLabel(volSnap, VolSyncDoNotDeleteLabel, VolSyncDoNotDeleteLabelVal)
	if labelsUpdated {
		if err := v.client.Update(v.ctx, volSnap); err != nil {
			v.log.Error(err, "Failed to add label to snapshot",
				"snapshot name", volSnap.GetName(), "labelName", VolSyncDoNotDeleteLabel)

			return nil, fmt.Errorf("failed to add %s label to snapshot %s (%w)",
				VolSyncDoNotDeleteLabel, volSnap.GetName(), err)
		}

		v.log.Info("label added to snapshot", "snapshot name", volSnap.GetName(), "labelName", VolSyncDoNotDeleteLabel)
	}

	v.log.V(1).Info("VolumeSnapshot validated", "volumesnapshot name", volSnap.GetName())

	return volSnap, nil
}

func (v *VSHandler) addLabel(obj client.Object, labelName, labelValue string) bool {
	labelsUpdated := false

	// Add Label to indicate that owner should not delete/cleanup this object
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	val, ok := labels[labelName]
	if !ok || val != labelValue {
		labels[labelName] = labelValue

		labelsUpdated = true

		obj.SetLabels(labels)
	}

	return labelsUpdated
}

func (v *VSHandler) addOwnerReference(obj, owner metav1.Object) (bool, error) {
	currentOwnerRefs := obj.GetOwnerReferences()

	err := ctrlutil.SetOwnerReference(owner, obj, v.client.Scheme())
	if err != nil {
		return false, fmt.Errorf("%w", err)
	}

	needsUpdate := !reflect.DeepEqual(obj.GetOwnerReferences(), currentOwnerRefs)

	return needsUpdate, nil
}

func (v *VSHandler) addLabelAndVRGOwnerRefAndUpdate(obj client.Object, labelName, labelValue string) error {
	labelsUpdated := v.addLabel(obj, labelName, labelValue)

	ownerRefUpdated, err := v.addOwnerReference(obj, v.owner) // VRG as owner
	if err != nil {
		return err
	}

	if labelsUpdated || ownerRefUpdated {
		objKindAndName := getKindAndName(v.client.Scheme(), obj)

		if err := v.client.Update(v.ctx, obj); err != nil {
			v.log.Error(err, "Failed to add label or VRG owner reference to obj", "obj", objKindAndName)

			return fmt.Errorf("failed to add %s label or VRG owner reference to %s (%w)", labelName, objKindAndName, err)
		}

		v.log.Info("label and VRG ownerRef added to object",
			"obj", objKindAndName, "labelName", labelName, "label value", labelValue)
	}

	return nil
}

func (v *VSHandler) addOwnerReferenceAndUpdate(obj client.Object, owner metav1.Object) error {
	needsUpdate, err := v.addOwnerReference(obj, owner)
	if err != nil {
		return err
	}

	if needsUpdate {
		objKindAndName := getKindAndName(v.client.Scheme(), obj)

		if err := v.client.Update(v.ctx, obj); err != nil {
			v.log.Error(err, "Failed to add owner reference to obj", "obj", objKindAndName)

			return fmt.Errorf("failed to add owner reference to %s (%w)", objKindAndName, err)
		}

		v.log.Info("ownerRef added to object", "obj", objKindAndName)
	}

	return nil
}

func (v *VSHandler) getRsyncServiceType() *corev1.ServiceType {
	// Use default right now - in future we may use a volsyncProfile
	return &DefaultRsyncServiceType
}

func (v *VSHandler) GetVolumeSnapshotClassFromPVCStorageClass(storageClassName *string) (string, error) {
	if storageClassName == nil || *storageClassName == "" {
		err := fmt.Errorf("no storageClassName given, cannot proceed")
		v.log.Error(err, "Failed to get StorageClass")

		return "", err
	}

	storageClass := &storagev1.StorageClass{}
	if err := v.client.Get(v.ctx, types.NamespacedName{Name: *storageClassName}, storageClass); err != nil {
		v.log.Error(err, "Failed to get StorageClass", "name", storageClassName)

		return "", fmt.Errorf("error getting storage class (%w)", err)
	}

	volumeSnapshotClasses, err := v.GetVolumeSnapshotClasses()
	if err != nil {
		return "", err
	}

	var matchedVolumeSnapshotClassName string

	for _, volumeSnapshotClass := range volumeSnapshotClasses {
		if volumeSnapshotClass.Driver == storageClass.Provisioner {
			// Match the first one where driver/provisioner == the storage class provisioner
			// But keep looping - if we find the default storageVolumeClass, use it instead
			if matchedVolumeSnapshotClassName == "" || isDefaultVolumeSnapshotClass(volumeSnapshotClass) {
				matchedVolumeSnapshotClassName = volumeSnapshotClass.GetName()
			}
		}
	}

	if matchedVolumeSnapshotClassName == "" {
		noVSCFoundErr := fmt.Errorf("unable to find matching volumesnapshotclass for storage provisioner %s",
			storageClass.Provisioner)
		v.log.Error(noVSCFoundErr, "No VolumeSnapshotClass found")

		return "", noVSCFoundErr
	}

	return matchedVolumeSnapshotClassName, nil
}

func isDefaultVolumeSnapshotClass(volumeSnapshotClass snapv1.VolumeSnapshotClass) bool {
	isDefaultAnnotation, ok := volumeSnapshotClass.Annotations[VolumeSnapshotIsDefaultAnnotation]

	return ok && isDefaultAnnotation == VolumeSnapshotIsDefaultAnnotationValue
}

func (v *VSHandler) GetVolumeSnapshotClasses() ([]snapv1.VolumeSnapshotClass, error) {
	if v.volumeSnapshotClassList == nil {
		// Load the list if it hasn't been initialized yet
		v.log.Info("Fetching VolumeSnapshotClass", "labelSelector", v.volumeSnapshotClassSelector)

		selector, err := metav1.LabelSelectorAsSelector(&v.volumeSnapshotClassSelector)
		if err != nil {
			v.log.Error(err, "Unable to use volume snapshot label selector", "labelSelector", v.volumeSnapshotClassSelector)

			return nil, fmt.Errorf("unable to use volume snapshot label selector (%w)", err)
		}

		listOptions := []client.ListOption{
			client.MatchingLabelsSelector{
				Selector: selector,
			},
		}

		vscList := &snapv1.VolumeSnapshotClassList{}
		if err := v.client.List(v.ctx, vscList, listOptions...); err != nil {
			v.log.Error(err, "Failed to list VolumeSnapshotClasses", "labelSelector", v.volumeSnapshotClassSelector)

			return nil, fmt.Errorf("error listing volumesnapshotclasses (%w)", err)
		}

		v.volumeSnapshotClassList = vscList
	}

	return v.volumeSnapshotClassList.Items, nil
}

func (v *VSHandler) getScheduleCronSpec() (*string, error) {
	if v.schedulingInterval != "" {
		return ConvertSchedulingIntervalToCronSpec(v.schedulingInterval)
	}

	// Use default value if not specified
	v.log.Info("Warning - scheduling interval is empty, using default Schedule for volsync",
		"DefaultScheduleCronSpec", DefaultScheduleCronSpec)

	return &DefaultScheduleCronSpec, nil
}

// Convert from schedulingInterval which is in the format of <num><m,h,d>
// to the format VolSync expects, which is cronspec: https://en.wikipedia.org/wiki/Cron#Overview
func ConvertSchedulingIntervalToCronSpec(schedulingInterval string) (*string, error) {
	// format needs to have at least 1 number and end with m or h or d
	if len(schedulingInterval) < SchedulingIntervalMinLength {
		return nil, fmt.Errorf("scheduling interval %s is invalid", schedulingInterval)
	}

	mhd := schedulingInterval[len(schedulingInterval)-1:]
	mhd = strings.ToLower(mhd) // Make sure we get lowercase m, h or d

	num := schedulingInterval[:len(schedulingInterval)-1]

	numInt, err := strconv.Atoi(num)
	if err != nil {
		return nil, fmt.Errorf("scheduling interval prefix %s cannot be convered to an int value", num)
	}

	var cronSpec string

	switch mhd {
	case "m":
		cronSpec = fmt.Sprintf("*/%s * * * *", num)
	case "h":
		// TODO: cronspec has a max here of 23 hours - do we try to convert into days?
		cronSpec = fmt.Sprintf("0 */%s * * *", num)
	case "d":
		if numInt > CronSpecMaxDayOfMonth {
			// Max # of days in interval we'll allow is 28 - otherwise there are issues converting to a cronspec
			// which is expected to be a day of the month (1-31).  I.e. if we tried to set to */31 we'd get
			// every 31st day of the month
			num = "28"
		}

		cronSpec = fmt.Sprintf("0 0 */%s * *", num)
	}

	if cronSpec == "" {
		return nil, fmt.Errorf("scheduling interval %s is invalid. Unable to parse m/h/d", schedulingInterval)
	}

	return &cronSpec, nil
}

func (v *VSHandler) IsRSDataProtected(pvcName string) (bool, error) {
	l := v.log.WithValues("pvcName", pvcName)

	// Get RD instance
	rs := &volsyncv1alpha1.ReplicationSource{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      getReplicationSourceName(pvcName),
			Namespace: v.owner.GetNamespace(),
		}, rs)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			l.Error(err, "Failed to get ReplicationSource")

			return false, fmt.Errorf("%w", err)
		}

		l.Info("No ReplicationSource found", "pvcName", pvcName)

		return false, nil
	}

	return isRSLastSyncTimeReady(rs.Status), nil
}

func isRSLastSyncTimeReady(rsStatus *volsyncv1alpha1.ReplicationSourceStatus) bool {
	if rsStatus != nil && rsStatus.LastSyncTime != nil && !rsStatus.LastSyncTime.IsZero() {
		return true
	}

	return false
}

func (v *VSHandler) getRDLatestImage(pvcName string) (*corev1.TypedLocalObjectReference, error) {
	l := v.log.WithValues("pvcName", pvcName)

	// Get RD instance
	rdInst := &volsyncv1alpha1.ReplicationDestination{}

	err := v.client.Get(v.ctx,
		types.NamespacedName{
			Name:      getReplicationDestinationName(pvcName),
			Namespace: v.owner.GetNamespace(),
		}, rdInst)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			l.Error(err, "Failed to get ReplicationDestination")

			return nil, fmt.Errorf("error getting replicationdestination (%w)", err)
		}

		l.Info("No ReplicationDestination found")

		return nil, nil
	}

	var latestImage *corev1.TypedLocalObjectReference
	if rdInst.Status != nil {
		latestImage = rdInst.Status.LatestImage
	}

	return latestImage, nil
}

// Returns true if at least one sync has completed (we'll consider this "data protected")
func (v *VSHandler) IsRDDataProtected(pvcName string) (bool, error) {
	latestImage, err := v.getRDLatestImage(pvcName)
	if err != nil {
		return false, err
	}

	return isLatestImageReady(latestImage), nil
}

func isLatestImageReady(latestImage *corev1.TypedLocalObjectReference) bool {
	if latestImage == nil || latestImage.Name == "" || latestImage.Kind != VolumeSnapshotKind {
		return false
	}

	return true
}

func addVRGOwnerLabel(owner, obj metav1.Object) {
	// Set vrg label to owner name - enables lookups by owner label
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[VRGOwnerLabel] = owner.GetName()
	obj.SetLabels(labels)
}

func getReplicationDestinationName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationDestination
}

func getReplicationSourceName(pvcName string) string {
	return pvcName // Use PVC name as name of ReplicationSource
}

// Service name that VolSync will create locally in the same namespace as the ReplicationDestination
func getLocalServiceNameForRDFromPVCName(pvcName string) string {
	return getLocalServiceNameForRD(getReplicationDestinationName(pvcName))
}

func getLocalServiceNameForRD(rdName string) string {
	// This is the name VolSync will use for the service
	return fmt.Sprintf("volsync-rsync-dst-%s", rdName)
}

// This is the remote service name that can be accessed from another cluster.  This assumes submariner and that
// a ServiceExport is created for the service on the cluster that has the ReplicationDestination
func getRemoteServiceNameForRDFromPVCName(pvcName, rdNamespace string) string {
	return fmt.Sprintf("%s.%s.svc.clusterset.local", getLocalServiceNameForRDFromPVCName(pvcName), rdNamespace)
}

func getKindAndName(scheme *runtime.Scheme, obj client.Object) string {
	ref, err := reference.GetReference(scheme, obj)
	if err != nil {
		return obj.GetName()
	}

	return ref.Kind + "/" + ref.Name
}

func objectRefMatches(a, b *corev1.TypedLocalObjectReference) bool {
	if a == nil {
		return b == nil
	}

	if b == nil {
		return false
	}

	return a.Kind == b.Kind && a.Name == b.Name
}
