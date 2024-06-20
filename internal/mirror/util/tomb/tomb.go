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

package tomb

import (
	"sync"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/util/log"
)

var callbacks teardownCallbacks

func init() {
	callbacks = teardownCallbacks{
		waitCh:        make(chan struct{}, 1),
		interruptedCh: make(chan struct{}, 1),
	}
}

type callback struct {
	Name string
	Do   func()
}

type teardownCallbacks struct {
	mutex    sync.RWMutex
	data     []callback
	exitCode int

	exhausted        bool
	notInterruptable bool

	waitCh        chan struct{}
	interruptedCh chan struct{}
}

func (c *teardownCallbacks) registerOnShutdown(name string, cb func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data = append(c.data, callback{Name: name, Do: cb})
	log.DebugF("teardown callback '%s' added, callbacks in queue: %d\n", name, len(c.data))
}

func (c *teardownCallbacks) shutdown(exitCode int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Prevent double shutdown.
	if c.exhausted {
		return
	}

	c.exitCode = exitCode

	log.DebugF("teardown started, queue length: %d\n", len(c.data))

	// Run callbacks in FIFO order to shutdown fundamental things last.
	for i := len(c.data) - 1; i >= 0; i-- {
		cb := c.data[i]
		log.DebugF("teardown callback %d: '%s' started\n", i, cb.Name)
		cb.Do()
		c.data[i] = callback{Name: "Stub", Do: func() {}}
		log.DebugF("teardown callback %d: '%s' done\n", i, cb.Name)
	}

	log.DebugLn("teardown is finished")
	c.exhausted = true
	close(c.waitCh)
}

func (c *teardownCallbacks) wait() {
	<-c.waitCh
}

func IsInterrupted() bool {
	select {
	case <-callbacks.interruptedCh:
		return true
	default:
	}
	return false
}
