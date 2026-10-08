## Iteration 1.
10 points

The Replicated Log should have the following deployment architecture: one Master and any number of Secondaries.

 
- Master should expose a simple HTTP server (or alternative service with a similar API) with: 
    - POST method - appends a message to the in-memory list
    - GET method - returns all messages from the in-memory list

 - Secondary should expose a simple  HTTP server(or alternative service with a similar API)  with:
    - GET method - returns all replicated messages from the in-memory list

Properties and assumptions:

- after each POST request, the message should be replicated on every Secondary server
- Master should ensure that Secondaries have received a message via ACK
- Master’s POST request should be finished only after receiving ACKs from all Secondaries (blocking replication approach)
- to test that the replication is blocking, introduce a delay/sleep on the Secondary
- at this stage, assume that the communication channel is a perfect link (no failures and messages lost)
- any RPC framework can be used for Master-Secondary communication (Sockets, language-specific RPC, HTTP, Rest, gRPC, …)
- your implementation should support logging 
- Master and Secondaries should run in Docker

## Testing
To test the system behaviour under different network failures in a predictable manner, it is proposed to additionally implement a test harness. 
It should allow simulating network failures programmatically and verifying the expected behaviour through integration tests. 
In the current iteration, it should be tested that replication runs in parallel and that the master waits for ACKs from all secondaries.

Possible Hareness test can be represented in the following way:
``` python
transport = MockedTransport()
masterNode = MasterNode(transport)
secondaryNode1 = SecondaryNode(transport)
secondaryNode2 = SecondaryNode(transport)

transport.setDelay(masterNode, secondaryNode1, 5 sec)

masterNode.appendMsg("m1") <- should be blocked for 5 sec
masterNode.listMsgs() -> "m1"

transport.setDelay(masterNode, secondaryNode2, 6 sec)

masterNode.appendMsg("m2") <- should be blocked for 6 sec
masterNode.listMsgs() -> "m1", "m2"

transport.removeDelay(masterNode, secondaryNode1)
transport.removeDelay(masterNode, secondaryNode2)

masterNode.appendMsg("m3") <- no blocking

masterNode.listMsgs() -> "m1", "m2", "m3"
secondaryNode1.listMsgs()  -> "m1", "m2", "m3"
secondaryNode2.listMsgs()  -> "m1", "m2", "m3"
```