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

// ConstraintPolicyRule defines a single constraint policy rule.
type ConstraintPolicyRule struct {
	//+kubebuilder:validation:Required
	Name string `json:"name"`

	//+kubebuilder:validation:Required
	Request string `json:"request"`

	//+kubebuilder:validation:Required
	Limit string `json:"limit"`
}

// ConstraintPolicyTable defines a array of policy rules used
// to manage the results of rule evalautions as part of the
// policy binding status.
type ConstraintPolicyTable struct {
	Rules []string `json:"rules"`
}

// ConstraintPolicyStatus defines a array of policy rules used
// to manage the results of rule evalautions as part of the
// policy binding status.
type ConstraintPolicyStatus struct {
	Table ConstraintPolicyTable `json:"table"`
}

// ConstraintPolicySpec defines the desired state of ConstraintPolicy.
type ConstraintPolicySpec struct {
	//+kubebuilder:validation:MinItems=1
	//+kubebuilder:validation:Required
	Rules []*ConstraintPolicyRule `json:"rules"`
}

// nolint:lll
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:servedversion
//+kubebuilder:resource:shortName=cp,scope=Namespaced,singular=constraintpolicy
//+kubebuilder:printcolumn:name="Rules",type="string",JSONPath=".status.table.rules",priority=1
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of constraint policy"

// ConstraintPolicy is the Schema for the constraintpolicies API.
type ConstraintPolicy struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConstraintPolicySpec   `json:"spec,omitempty"`
	Status ConstraintPolicyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ConstraintPolicyList contains a list of ConstraintPolicy.
type ConstraintPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ConstraintPolicy `json:"items"`
}

// nolint:gochecknoinits
func init() {
	SchemeBuilder.Register(&ConstraintPolicy{}, &ConstraintPolicyList{})
}
