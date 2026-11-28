# Known issues

## Two concurrent calls can overcommit a pool

The controller checks the free space of a pool and records the volume under a lock inside its own
process. csi-provisioner, csi-attacher and csi-resizer elect their leaders independently. So with
`controller.replicas` above 1, two replicas can handle two calls for the same pool: for example,
one creates a volume while the other resizes a volume or moves one in. Both pass the check.

This needs two such calls for one pool within milliseconds, and a pool that is nearly full. The
pool is then overcommitted by the size of one volume. The agent does not check the total.

With `controller.replicas: 1`, this can happen only during a rollout, while the old pod and the
new pod run side by side.

## A wrong node clock counts right after the controller starts

The first report that the controller sees from a node is as old as the time in the report says.
See [Agent heartbeat](architecture.md#agent-heartbeat).

- If the clock of a node is more than `controller.agentTimeoutSeconds` behind, the controller
  treats the node as dead until its agent reports again, within 30 seconds. No volume is created
  on the node meanwhile.
- If the clock of a node is ahead and its agent died before the controller started, the
  controller treats the node as alive for up to `controller.agentTimeoutSeconds` after it starts.
