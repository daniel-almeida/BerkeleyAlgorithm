#### Implementation (Golang) of the Berkeley algorithm for clock synchronization

**Description**

"Master queries all of the slave nodes for their local time and each node replies with their current time.  The master then computes the time difference between its local clock and that of each of the slaves (deriving a set of delta values, one per slave). Next, the master computes avg, a fault tolerant average of the delta values: an average over the largest set of delta values that differ from one another by at most a pre-defined constant d. Then, for each slave the master computes a time correction value by computing the difference between avg and the delta value for the slave. Finally, the master sends this correction value to each slave for the slaves to adjust their clock."

**Details:**

[1] https://en.wikipedia.org/wiki/Berkeley_algorithm

[2] Original paper: The Accuracy of the Clock Synchronization Achieved by TEMPO in Berkeley UNIX 4.3BSD (http://dl.acm.org/citation.cfm?id=69640)

**Features and limitations:**
1. Handles message loss and delay

2. Tolerates slave failures

**Assumptions:**
1. Master never fails

2. List of slaves is known and fixed

3. Clock values are monotonically increasing integers


**For help with building/compiling:** https://golang.org/doc/code.html

**Running Master**

\# ./BerkeleyAlgorithm \<MODE FLAG (-m)\> \<LISTEN ADDRESS (IP:PORT)\> \<INITIAL LOCAL CLOCK/TIME\> \<THRESHOLD FOR FAULT-TOLERANT AVERAGE\> \<FILE WITH LIST OF SLAVES\>

Example:

    ./BerkeleyAlgorithm -m 127.0.0.1:8000 1 1500 slavesfile

**Running Slave**

\# ./BerkeleyAlgorithm \<MODE FLAG (-s)\> \<LISTEN ADDRESS (IP:PORT)\> \<INITIAL LOCAL CLOCK/TIME\>

Example (3 slaves)

    ./BerkeleyAlgorithm -s 127.0.0.1:8001 1
    ./BerkeleyAlgorithm -s 127.0.0.1:8002 1
    ./BerkeleyAlgorithm -s 127.0.0.1:8003 1
