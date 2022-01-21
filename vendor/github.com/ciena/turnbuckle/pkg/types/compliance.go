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
imitations under the License.
*/

package types

// ComplianceLevel constrained string type for value checking.
type ComplianceLevel string

// String constants for the policy compliance.
const (
	ComplianceNone      = ComplianceLevel("")
	CompliancePending   = "Pending"
	ComplianceCompliant = "Compliant"
	ComplianceLimit     = "Limit"
	ComplianceViolation = "Violation"
	ComplianceError     = "Error"
)

// nolint:gochecknoglobals, gomnd
var complianceSeverityMap = map[ComplianceLevel]int{
	ComplianceNone:      0,
	CompliancePending:   0,
	ComplianceCompliant: 1,
	ComplianceLimit:     2,
	ComplianceViolation: 3,
	ComplianceError:     4,
}

// String returns the string representation of the compliance level.
func (cl ComplianceLevel) String() string {
	return string(cl)
}

// CompareComplianceSeverity compares two compliance level and return an
// integer value where if the returned value is < 0 then the first
// parameter has a higher severity level, > 0 then the second parameter
// has a higher severity level, or 0 if the the values are equal.
func CompareComplianceSeverity(left, right string) int {
	leftVal, okLeft := complianceSeverityMap[ComplianceLevel(left)]
	rightVal, okRight := complianceSeverityMap[ComplianceLevel(right)]

	if okLeft && !okRight {
		return -1
	}

	if !okLeft && okRight {
		return 1
	}

	if !okLeft && !okRight {
		return 0
	}

	return rightVal - leftVal
}
