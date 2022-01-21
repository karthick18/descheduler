/*
Copyright 2021 Ciena Corporation.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Policy used to type a policy name so that if can be validated with a pattern.
//+kubebuilder:validation:Pattern:=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
type Policy string

// ConstraintPolicyOfferTarget identifies a target of a policy offer.
type ConstraintPolicyOfferTarget struct {

	// Name defines the optional name for the target specification/
	//+optional
	Name string `json:"name"`

	// ApiVersion used to help identify the Kubernetes resource type
	// to select.
	//+optional
	APIVersion string `json:"apiVersion"`

	// Kind use to help identify the Kubernetes resource type to select.
	//+kubebuilder:validation:Required
	//+kubebuilder:validation:Enum=Pod;Cluster;Endpoint;NetworkService
	Kind string `json:"kind"`

	// Mode hint used when selecting more abstract concepts such as a
	// network chain to help identify which element of the abstract
	// should be used for reference.
	//+optional
	//+kubebuilder:validation:Required
	//+kubebuilder:validation:Enum=Start;End
	Mode string `json:"mode"`

	// LabelSelector is used to select the Kubernetes resources to
	// include as part of the target.
	//+optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector"`
}

// ConstraintPolicyOfferSpec defines the desired state of ConstraintPolicyOffer.
type ConstraintPolicyOfferSpec struct {

	// Targets list of targets to be included in the offer.
	//+kubebuilder:validation:Required
	Targets []*ConstraintPolicyOfferTarget `json:"targets"`

	// Policies list of policies included in the offer.
	//+kubebuilder:validation:Required
	Policies []Policy `json:"policies"`

	// Period defines how often the policies included in the offer should be
	// evaluated.
	//+kubebuilder:validation:Required
	//+kubebuilder:validation:Pattern:=`^([0-9]+\.)?[0-9]+(h|m|s|us|µs|ms|ns)+$`
	Period string `json:"period"`

	// Garce defines how long a policy must be out of range before the policy
	// is considered in violation.
	//+kubebuilder:validation:Required
	//+kubebuilder:validation:Pattern:=`^([0-9]+\.)?[0-9]+(h|m|s|us|µs|ms|ns)+$`
	Grace string `json:"grace"`

	// ViolationPolicy defines the action that should be taken when a policy
	// is in violation.
	//+kubebuilder:validation:Required
	//+kubebuilder:validation:Enum=Ignore;Mediate;Evict
	ViolationPolicy string `json:"violationPolicy"`
}

// ConstraintPolicyOfferStatus defines the observed state of ConstraintPolicyOffer.
type ConstraintPolicyOfferStatus struct {

	// BindingCount summary of how many bindings were created from this offer.
	BindingCount int `json:"bindingCount"`

	// CompliantBindingCount summary of how many of the bindings are currently
	// compliant.
	CompliantBindingCount int `json:"compliantBindingCount"`

	// BindingSelector label used to locate (reference) the bindings created
	// by this offer.
	BindingSelector string `json:"bindingSelector"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=cpo,scope=Namespaced,singular=constraintpolicyoffer
//+kubebuilder:printcolumn:name="Policies",type="string",JSONPath=".spec.policies",priority=1
//+kubebuilder:printcolumn:name="Period",type="string",JSONPath=".spec.period",priority=1
//+kubebuilder:printcolumn:name="Grace",type="string",JSONPath=".spec.grace",priority=0
//+kubebuilder:printcolumn:name="ViolationPolicy",type="string",JSONPath=".spec.violationPolicy",priority=1
//+kubebuilder:printcolumn:name="Count",type="integer",JSONPath=".status.bindingCount",priority=0
//+kubebuilder:printcolumn:name="Compliant",type="integer",JSONPath=".status.compliantBindingCount",priority=0
//+kubebuilder:printcolumn:name="Selector",type="string",JSONPath=".status.bindingSelector",priority=1
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",priority=0
//+kubebuilder:printcolumn:name="Targets",type="string",JSONPath=".spec.targets",priority=1
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch

// ConstraintPolicyOffer is the Schema for the constraintpolicyoffers API.
//+genclient.
type ConstraintPolicyOffer struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConstraintPolicyOfferSpec   `json:"spec,omitempty"`
	Status ConstraintPolicyOfferStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ConstraintPolicyOfferList contains a list of ConstraintPolicyOffer.
type ConstraintPolicyOfferList struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []*ConstraintPolicyOffer `json:"items"`
}

// nolint:gochecknoinits
func init() {
	SchemeBuilder.Register(&ConstraintPolicyOffer{}, &ConstraintPolicyOfferList{})
}
