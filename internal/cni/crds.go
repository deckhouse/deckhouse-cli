package cni

const (
	CNIMigrationCRD = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cnimigrations.deckhouse.io
spec:
  group: deckhouse.io
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
  name: cninodemigrations.deckhouse.io
spec:
  group: deckhouse.io
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
    - cninm
`
)
