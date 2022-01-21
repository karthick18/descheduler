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
	"github.com/ciena/turnbuckle/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConstraintPolicyBindingSpec defines the desired state of ConstraintPolicyBinding.
type ConstraintPolicyBindingSpec struct {

	// Targets the list of targets for the binding
	//+kubernetes:validate:Required
	Targets map[string]types.Reference `json:"targets"`

	// Offer references the offer from which this binding is created in a
	// cluster/namespace/mame form.
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Offer string `json:"offer"`
}

// ConstraintPolicyBindingStatusRuleDetail contains the compliance information
// for a given rule.
type ConstraintPolicyBindingStatusRuleDetail struct {
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Rule string `json:"rule"`

	//+kubebuilder:validate:Required
	//+kubebuilder:validation:Enum=Error;Pending;Compliant;Limit;Violation
	Compliance string `json:"compliance"`

	//+optional
	//+nullable
	Reason string `json:"reason"`
}

// ConstraintPolicyBindingStatusDetail contains the summary compliance
// status of a binding as well as the detail from which it is summarized.
type ConstraintPolicyBindingStatusDetail struct {
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Policy string `json:"policy"`

	//+kubebuilder:validate:Required
	//+kubebuilder:validation:Enum=Error;Pending;Compliant;Limit;Violation
	Compliance string `json:"compliance"`

	//+optional
	//+nullable
	Reason string `json:"reason"`

	RuleDetails []*ConstraintPolicyBindingStatusRuleDetail `json:"ruleDetails"`
}

// ConstraintPolicyBindingStatus defines the observed state of ConstraintPolicyBinding.
type ConstraintPolicyBindingStatus struct {

	//+kubebuilder:validate:Required
	//+kubebuilder:validation:Enum=Error;Pending;Compliant;Limit;Violation
	Compliance string `json:"compliance"`

	//+optional
	FirstReason string `json:"firstReason"`

	//+optional
	Details []*ConstraintPolicyBindingStatusDetail `json:"details"`

	//+optional
	//+nullable
	LastComplianceChangeTimestamp metav1.Time `json:"lastComplianceChangeTimestamp,omitempty"`

	//+optional
	//+nullable
	LastMitigatedTimestamp metav1.Time `json:"lastMitigatedTimestamp,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=cpb,scope=Namespaced,singular=constraintpolicybinding
//+kubebuilder:printcolumn:name="Offer",type="string",JSONPath=".spec.offer",priority=0
//+kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.compliance",priority=0
//+kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.firstReason",priority=0
//+kubebuilder:printcolumn:name="LastChange",type="date",JSONPath=".status.lastComplianceChangeTimestamp",priority=1
//+kubebuilder:printcolumn:name="LastMitigation",type="date",JSONPath=".status.lastMitigatedTimestamp",priority=1
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",priority=0
//+kubebuilder:printcolumn:name="Targets",type="string",JSONPath=".spec.targets",priority=1
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch

// ConstraintPolicyBinding is the Schema for the constraintpolicybindings API.
//+genclient.
type ConstraintPolicyBinding struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ObjectMeta `json:"metadata,omitempty"`

	//+kubernetes:validation:Required
	Spec   ConstraintPolicyBindingSpec   `json:"spec,omitempty"`
	Status ConstraintPolicyBindingStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ConstraintPolicyBindingList contains a list of ConstraintPolicyBinding.
type ConstraintPolicyBindingList struct {
	metav1.TypeMeta `json:",inline"`
	// nolint:tagliatelle
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []*ConstraintPolicyBinding `json:"items"`
}

// nolint:gochecknoinits
func init() {
	SchemeBuilder.Register(&ConstraintPolicyBinding{}, &ConstraintPolicyBindingList{})
}
