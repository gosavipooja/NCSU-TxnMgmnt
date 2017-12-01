Team F


Members:
	Chirag Jain (csjain)
	Dipansha Gupta (dgupta10)
	Guru Darshan Pollepalli Manohara (gpollep)
	Pooja Gosavi (pigosavi)


List of files changed:
	buffer/BasicBufferMgr.java
	buffer/Buffer.java
	buffer/BufferMgr.java
	log/LogMgr.java
	server/SimpleDB.java (minor modification)


Changes in SimpleDB.java:
	The earlier implementation of SimpleDB.initFileLogAndBufferMgr() was initializing BufferMgr after LogMgr. 
	With the new changes, LogMgr will request a buffer pool from BufferMgr, so BufferMgr had to initialized first. Hence the order of initialization of LogMgr and BufferMgr was reversed in the code.


Buffer Manager Test Cases:

	All the buffer manager test cases work by first pinning blocks 11-17. 
	One of the block in the buffer pool is allocated to log manager. Hence only 7 free blocks are available for experimentation.
	The tests take a snapshot of the buffer pool before and after the replacement.
	At the end of the test, the test cases perform a diff between the snapshots of the buffer pool before and after the replacement and dump the changes. 
	Every test case ends with the statement "Changes in Buffer Pool" which lists the changes in the buffer pool
	
	All these tests have been documented in the spreadsheet "Buffer Test Results.xlsx"


Log Manager Test Cases:

	To test if the log manager functioned as expected, we first executed SimpleDB server without any of our changes and ran SimpleDB client which created the student table.
	Later we repeated the same experiment with our changes added into SimpleDB server. A binary comparison was performed between the simpledb.log files obtained from both the experiments
	Both were an exact match. Hence it was concluded that the modified log manager is able to function with the newly added changes without compromising on the functionality.



Notes about test case provided in the appendix:
	The test case provided in the appendix is tested and its results are documented in the spreadsheet "Buffer Test Results.xlsx". 
	The test case was modified to not pin block 8 because of the new modifications to LogMgr. LogMgr always has a buffer in buffer pool. Hence only 7 buffers are available for applications.
	Pinning a new buffer would cause BufferAbortException if no buffers were unpinned. Though the block 8 was removed from the picture, the results of the replacement were not affected by it and Block 5 was replaced eventually.