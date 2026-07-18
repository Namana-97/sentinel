// Package v1 contains the Sentinel Kubernetes API.
// +kubebuilder:object:generate=true
// +groupName=sentinel.io
package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion identifies the v1 Sentinel API.
	GroupVersion = schema.GroupVersion{Group: "sentinel.io", Version: "v1"}
	// SchemeBuilder registers Sentinel API types.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds all Sentinel API types to a runtime scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
