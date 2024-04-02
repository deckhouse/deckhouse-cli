# Virtualization
Subcommand for the command line client for Deckhouse.
Manages virtual machine-related operations in your Kubernetes cluster.

### Available Commands:
* console      - Connect to a console of a virtual machine.
* port-forward - Forward local ports to a virtual machine
* scp          - SCP files from/to a virtual machine.
* ssh          - Open an ssh connection to a virtual machine.
* vnc          - Open a vnc connection to a virtual machine.

### Examples
#### console
```shell
d8 virtualziation console myvm
d8 virtualziation console myvm.mynamespace
```
#### port-forward
```shell
d8 virtualziation port-forward myvm tcp/8080:8080
d8 virtualziation port-forward --stdio=true myvm.mynamespace 22
```
#### scp
```shell
d8 virtualziation scp myfile.bin user@myvm:myfile.bin
d8 virtualziation scp user@myvm:myfile.bin ~/myfile.bin
```
#### ssh
```shell
d8 virtualziation --identity-file=/path/to/ssh_key ssh user@myvm.mynamespace
d8 virtualziation ssh --local-ssh=true --namespace=mynamespace --username=user myvm
```
#### vnc
```shell
d8 virtualziation vnc myvm.mynamespace
d8 virtualziation vnc myvm -n mynamespace
```