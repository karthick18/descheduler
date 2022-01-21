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

package types

import (
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"

	uv1 "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/rand"
)

const separator = ":"

// Reference defines a cluster specific reference to a resource.
type Reference struct {
	// Cluster represents the resolved reference to a cluster ID
	Cluster string `json:"cluster"`

	// APIVersion represents the resolved APIVersion of the reference
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-zA-Z_][a-zA-Z0-9_-]*$`
	APIVersion string `json:"apiVersion"`

	// Kind represents the resolved Kind of the reference
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-zA-Z_][a-zA-Z0-9_-]*$`
	Kind string `json:"kind"`

	// Namespace represents the resolved Namespace of the reference
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-zA-Z_][a-zA-Z0-9_-]*$`
	Namespace string `json:"namespace"`

	// Name represents the resolved Name of the reference
	//+kubernetes:validate:Required
	//+kubebuilder:validation:Pattern:=`^[a-zA-Z_][a-zA-Z0-9_-]*$`
	Name string `json:"name"`
}

// ReferenceList defines a slice of References.
type ReferenceList []*Reference

// ReferenceListMap defines a map from a string (target name)
// to a ReferenceList.
type ReferenceListMap map[string]ReferenceList

// AsBindingName creates a name that can be used for a policy binding
// based on the given offer compined with the Reference instances in the
// list.
func (r ReferenceList) AsBindingName(offerName string) string {
	hash := fnv.New32a()
	for _, ref := range r {
		hash.Write([]byte(ref.String()))
	}

	return offerName + "-" + rand.SafeEncodeString(fmt.Sprint(hash.Sum32()))
}

// Contains returns true if the given reference is in the ReferenceList
// else false.
func (r ReferenceList) Contains(ref *Reference) bool {
	for _, have := range r {
		if *have == *ref {
			return true
		}
	}

	return false
}

// Permutations generates all the permutations of the ReferenceListMap where
// the map key name represents a set. The generated permutations contain an
// entry from each set and are added to permutation in alphabetical order base
// on the map key that represents that set.
func (m ReferenceListMap) Permutations() ([]string, []ReferenceList) {
	// Define a closer (nested function) that is used to increment
	// the counters that represent the iterators through the permutations
	var inc func(list []ReferenceList, refIdxs []int)
	inc = func(list []ReferenceList, refIdxs []int) {
		for i := len(refIdxs) - 1; i >= 0; i-- {
			if i == 0 || refIdxs[i] < len(list[i])-1 {
				refIdxs[i]++
				return
			}
			refIdxs[i] = 0
		}
	}

	// if any of the map entries are empty then we have no permutations
	if len(m) == 0 {
		return []string{}, []ReferenceList{}
	}

	for _, v := range m {
		if len(v) == 0 {
			return []string{}, []ReferenceList{}
		}
	}

	// Sort the keys in the map so we have consistent binding naming
	// conventions
	i := 0
	keys := make([]string, len(m))

	for key := range m {
		keys[i] = key
		i++
	}

	sort.Slice(keys, func(a, b int) bool {
		return keys[a] < keys[b]
	})

	// In key order add each references object to an internal referenes list
	list := make([]ReferenceList, len(keys))
	for i, key := range keys {
		list[i] = m[key]
	}

	// Generate permutation slice.
	var permutations []ReferenceList

	refIdxs := make([]int, len(m))

	for ; refIdxs[0] < len(list[0]); inc(list, refIdxs) {
		// create a permutation from the current index values and
		// append it to the list
		var permutation ReferenceList
		for i := 0; i < len(list); i++ {
			permutation = append(permutation, list[i][refIdxs[i]])
		}

		permutations = append(permutations, permutation)
	}

	return keys, permutations
}

// String marshal a Reference value into a string.
func (t *Reference) String() string {
	var buf strings.Builder

	buf.WriteString(t.Cluster)
	buf.WriteString(separator)
	buf.WriteString(t.Namespace)
	buf.WriteString(separator)
	buf.WriteString(t.APIVersion)
	buf.WriteString(separator)
	buf.WriteString(t.Kind)
	buf.WriteString(separator)
	buf.WriteString(t.Name)

	return buf.String()
}

// the regex expression used to parse a string representation of a Reference.
// nolint:lll
var referenceRE = regexp.MustCompile(`^((([a-zA-Z_][a-zA-Z0-9-_]*)?:)?([a-zA-Z_][a-zA-Z0-9-_]*)?:)?([a-zA-Z_][a-zA-Z0-9-_\/\.]*):([A-Z][a-zA-Z0-9]*):([a-zA-Z_][a-zA-Z0-9-_]*)$`)

// Indexes into the regexp results to get the various parts.
const (
	IndexCluster    = 3
	IndexNamespace  = 4
	IndexAPIVersion = 5
	IndexKind       = 6
	IndexName       = 7
)

// ErrParseReference returned when a given string cannot be parsed as a Reference.
var ErrParseReference = errors.New("parse-reference")

// ErrConvertReference return when an unstructured object does not have
// complete information to be converted to a Reference instance.
var ErrConvertReference = errors.New("convert-unstructured-to-reference")

// ParseReference attempts to parse the given string as a Reference and returns
// the value or an error if it cannot be parsed as a Reference.
func ParseReference(in string) (*Reference, error) {
	var t Reference

	parts := referenceRE.FindStringSubmatch(in)
	if len(parts) == 0 {
		return nil, ErrParseReference
	}

	t.Cluster = parts[IndexCluster]
	t.Namespace = parts[IndexNamespace]
	t.APIVersion = parts[IndexAPIVersion]
	t.Kind = parts[IndexKind]
	t.Name = parts[IndexName]

	return &t, nil
}

// NewReferenceFromUnstructured creates and returns a new Reference instance
// from the given unstructured resource information.
func NewReferenceFromUnstructured(in uv1.Unstructured) *Reference {
	out := Reference{}
	if val, ok := in.Object["apiVersion"].(string); ok {
		out.APIVersion = val
	}

	if val, ok := in.Object["kind"].(string); ok {
		out.Kind = val
	}

	if md, ok := in.Object["metadata"].(map[string]interface{}); ok {
		if val, ok := md["namespace"].(string); ok {
			out.Namespace = val
		}

		if val, ok := md["name"].(string); ok {
			out.Name = val
		}
	}

	return &out
}
