
Buffer Manager Test Cases:
All the buffer manager test cases work by first pinning blocks 11-17. 
One of the block in the buffer pool is allocated to log manager. Hence only 7 free blocks are available for experimentation.
The tests take a snapshot of the buffer pool before and after the replacement.
At the end of the test, the test cases perform a diff between the snapshots of the bufferpool before and after the replacement and dump the changes. 
Every test case ends with the statement "Changes in Buffer Pool" which lists the changes in the buffer pool
All these changes have been documented in "Buffer Test Results.xlsx"