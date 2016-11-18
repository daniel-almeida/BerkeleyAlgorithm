/* CPSC 538B - Distributed Systems. Daniel Almeida - Assignment 3 */

This version of Ricart-Agrawala++ can handle failures of any nodes in the network (e.g., Portal Node or regular Nodes) and allows new nodes to join the network by contacting any existing nodes. A known limitation (further explained below) is the possibility of conflicting Node IDs when multiple nodes join the network simultaneously through different Portal Nodes.

JOINING

New nodes are able to join the network by contacting any node in the network. The Join operation is as follows:

1) A New Node sends a JOIN message to an existing node (Portal Node) and starts a timer with duration RTT.
2) Portal Node replies with an ACCEPT message, which contains a Node Id* assigned to the New Node and a list of all nodes in the network. If the Portal Node doesn't reply in RTT ms, New Node considers the Portal Node to have failed and exists the Joining procedure. No further actions are necessary because the other nodes in the network haven't been informed of New Node's intention of joining the network.
3) New Node sends a NEW NODE message to each node in the list received from Portal Node
4) Each active node in the network adds New Node to their own lists and replies with a CONFIRM NEW NODE message. At this point all existing nodes in the network recognize New Node. New Node is already able to REPLY to requests from other nodes.
5) Upon receiving a CONFIRM NEW NODE message, New Node confirm the sender of that message as an active Node in his own list of nodes.

At this point New Node is known by all other nodes in the network and can invoke the Critical Section (CS).
* Currently, the Node Id is assigned by the Portal node based on the highest known Node Id. Nodes update the highest known Node Id whenever they receive messages from other nodes in the network. A known limitation of the current implementation is that nodes joining simultaneously through different Portal Nodes might be assigned the same Node ID. To solve this issue, I plan on moving the Joining process from the New Node to the Portal Node. The Portal Node is going to be responsible for requesting the execution of a Critical Section, which allows it to add the New Node to its own list of nodes and synchronize with all other nodes in the network before exiting the Critical Section.

NODE FAILURES

To deal with Node failures, a Node A that wants to enter the CS uses a timeout when sending out REQUEST messages to all other nodes in the network. A Node B is considered to have failed if it takes longer than RTT + CSSleepTime to send a REPLY message back to Node A. Once that happens, Node A sends an ARE YOU THERE message to Node B and starts the timer again with duration RTT. If Node B is actually alive, there are two possible responses: 
1) If Node B has priority to join the CS, it sends an I AM HERE message back to Node A. The timer will be reset and Node A will continue to send ARE YOU THERE messages for every timeout until it receives a REPLY message.
2) If Node B is not currently executing the CS or doesn't have priority to do so, it sends back a REPLY message to Node A. This situation might happen if the REQUEST message (from Node A to Node B) or the original REPLY message (from Node B to Node A) never arrived.

If Node B doesn't reply to one of Node A's ARE YOU THERE messages, it is considered to have failed. Node A will remove Node B from its list of nodes and decrement the number of oustanding REPLY messages, allowing Node A to enter the CS if the any outstanding REPLY message was the one that Node B should've sent back to Node A.
