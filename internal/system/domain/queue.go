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

package domain

// Queue represents a Deckhouse queue
type Queue struct {
	Name   string
	Status QueueStatus
	Items  []QueueItem
}

// QueueStatus represents queue status
type QueueStatus string

const (
	QueueStatusActive QueueStatus = "Active"
	QueueStatusPaused QueueStatus = "Paused"
)

// QueueItem represents an item in the queue
type QueueItem struct {
	Name       string
	Module     string
	HookName   string
	Binding    string
	Properties map[string]interface{}
}

// QueueListResult represents result of listing queues
type QueueListResult struct {
	Queues []Queue
	Error  error
}

