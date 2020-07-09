# riak_dist_protocols
Implementations of distribution protocols using a simple key-value store based on raik-core.

# Two-Phase Commit (2PC)
This repository includes an implementation of the 2PC protocol. It demonstrates the use of the gen_statem behaviour for implement a transaction manager.
It also demonstrates the use riak_core_vnode for implementing transaction cohorts that own the data.


# Generalized Paxos Atomic Commit of VLDB 2019
This protocol, that was published in VLDB 2019, supports running transactions on partitioned and replicated data.
While the common way for handling this kind of databases by combining Two-Phase Commit (2PC_ with Paxos, this approach suffer from additional round-trip cost.
The GPAC protocol reduces round-trip communication by combining the leader election phase of Paxos with the value discovery phase of 2PC.
The client communicates directly with all the replicas and the paper describes the size of the qourum required in each phase.

