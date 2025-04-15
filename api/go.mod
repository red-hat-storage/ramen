module github.com/ramendr/ramen/api

// Required minimum version, must be available in downstream builders.
go 1.23.5

// Recommended version: latest go 1.23 release.
toolchain go1.23.7

require (
	k8s.io/api v0.29.0
	k8s.io/apimachinery v0.29.0
	k8s.io/component-base v0.28.3
	sigs.k8s.io/controller-runtime v0.16.3
)

require (
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	golang.org/x/net v0.36.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
)

// replace directives to accommodate for stolostron
replace k8s.io/client-go v12.0.0+incompatible => k8s.io/client-go v0.29.0
