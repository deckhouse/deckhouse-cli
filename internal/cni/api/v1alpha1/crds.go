/*
Copyright 2025 Flant JSC

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

const (
	CNIMigrationCRD = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cnimigrations.network.deckhouse.io
spec:
  group: network.deckhouse.io
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                targetCNI:
                  type: string
                previousCNI:
                  type: string
            status:
              type: object
              properties:
                phase:
                  type: string
                nodes:
                  type: array
                  items:
                    type: object
                    properties:
                      name:
                        type: string
                      phase:
                        type: string
  scope: Cluster
  names:
    plural: cnimigrations
    singular: cnimigration
    kind: CNIMigration
    shortNames:
    - cnimig
`
	CNINodeMigrationCRD = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cninodemigrations.network.deckhouse.io
spec:
  group: network.deckhouse.io
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                nodeName:
                  type: string
            status:
              type: object
              properties:
                phase:
                  type: string
                message:
                  type: string
  scope: Namespaced
  names:
    plural: cninodemigrations
    singular: cninodemigration
    kind: CNINodeMigration
    shortNames:
    - cninmig
`
)
