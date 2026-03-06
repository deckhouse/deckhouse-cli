/*
Copyright 2024 Flant JSC

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

package modules

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

type VersionConstraint interface {
	Match(version interface{}) bool
	IsExact() bool
	HasChannelAlias() bool
}

type SemanticVersionConstraint struct {
	constraint *semver.Constraints
}

func NewSemanticVersionConstraint(c string) (*SemanticVersionConstraint, error) {
	constraint, err := semver.NewConstraint(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}
	return &SemanticVersionConstraint{constraint: constraint}, nil
}

func (s *SemanticVersionConstraint) HasChannelAlias() bool {
	return false
}

func (s *SemanticVersionConstraint) Match(version interface{}) bool {
	switch v := version.(type) {
	case *semver.Version:
		return s.constraint.Check(v)
	default:
		return false
	}
}

func (s *SemanticVersionConstraint) IsExact() bool {
	return false
}

type ExactTagConstraint struct {
	tag     string
	channel string
}

func (e *ExactTagConstraint) Tag() string {
	return e.tag
}

func (e *ExactTagConstraint) Channel() string {
	return e.channel
}

func NewExactTagConstraint(tag string) *ExactTagConstraint {
	return &ExactTagConstraint{tag: tag}
}

func NewExactTagConstraintWithChannel(tag string, channel string) *ExactTagConstraint {
	return &ExactTagConstraint{tag: tag, channel: channel}
}

func (e *ExactTagConstraint) Match(version interface{}) bool {
	switch v := version.(type) {
	case string:
		return e.tag == v
	default:
		return false
	}
}

func (e *ExactTagConstraint) IsExact() bool {
	return true
}

func (e *ExactTagConstraint) HasChannelAlias() bool {
	return e.channel != ""
}
