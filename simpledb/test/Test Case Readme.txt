
Buffer Manager Test Cases:
All the buffer manager test cases work by first pinning blocks 11-17. 
One of the block in the buffer pool is allocated to log manager. Hence only 7 free blocks are available for experimentation.
The tests take a snapshot of the buffer pool before and after the replacement.
At the end of the test, the test cases perform a diff between the snapshots of the bufferpool before and after the replacement and dump the changes. 
Every test case ends with the statement "Changes in Buffer Pool" which lists the changes in the buffer pool


--------------------------------------------------------------------------------------------

Test 1

Title: Basic Replacement Test

Test Description:
1) All the blocks in the buffer pool (except log manager) are pinned once in order 11 to 17
2) Block 15 is unpinned 
3) Block 18 will be newly pinned

Expectation:
1) Block 18 will replace Block 15 in the buffer pool

Explanation:
Block 15 is the only unpinned block available in the buffer pool
Hence it will be replaced

--------------------------------------------------------------------------------------------

Test 2

Title: Basic Replacement Test 2

Test Description:
1) All the blocks in the buffer pool (except log manager) are pinned once in order 11 to 17
2) Block 17 is unpinned
3) Block 15 is unpinned 
4) Block 18 will be newly pinned

Expectation:
1) Block 18 will replace Block 15 in the buffer pool

Explanation:
Both Block 15 and Block 17 have their Backward 2 distance as infinity.
So traditional LRU will be used and Block 15 will be evicted because
during pinning the blocks, Block 15 was pinned before Block 17
	 
--------------------------------------------------------------------------------------------
	 
Test 3

Title: LRU2 Replacement Test 1

Test Description:
1) All the blocks in the buffer pool (except log manager) are pinned once in order 11 to 17
2) Block 17 is pinned
3) Block 15 is pinned
4) Block 17 is unpinned twice
5) Block 15 is unpinned twice
6) Block 18 will be newly pinned

Expectation:
1) Block 18 will replace Block 15 in the buffer pool

Explanation:
Both Block 15 and Block 17 were pinned twice and unpinned twice to ensure that the pin count remains zero at the end.
Block 15 will be replaced because Backward 2 distance of Block 15 is higher than Block 17 (as it was pinned first during initialization)
	 
--------------------------------------------------------------------------------------------
	 
Test 4

Title: LRU2 Replacement Test 2

Test Description:
1) All the blocks in the buffer pool (except log manager) are pinned once in order 11 to 17
2) Block 17 is pinned twice
3) Block 15 is pinned twice
4) Block 17 is unpinned thrice
5) Block 15 is unpinned thrice
6) Block 18 will be newly pinned
7) Block 19 will be newly pinned

Expectation:
1) Block 18 will replace Block 17 in the buffer pool
2) Block 19 will replace Block 15 in the buffer pool

Explanation:
Both Block 15 and Block 17 were pinned thrice and unpinned thrice to ensure that the pin count remains zero at the end.
Block 15 will be replaced because Backward 2 distance of Block 17 is higher than Block 15 (as the second pin of Block 17 was before Block 15)

--------------------------------------------------------------------------------------------
	 
Test 5

Title: Master LRU Test (From project appendix)

Test Description:
1) Pin order = {11,12,13,14,15,16,17,14,12,17,11}
2) Unpin order = {17,16,15,14,11,17,14,12,12}

Expectation:
2) Block 19 will replace Block 15 in the buffer pool

Explanation:
Block 4, 2, 7 and 1 have second last access times. Block 1 has the largest backward 2 distance.
But the backward distance of Block 5 is at infinity. Hence Block 5 will be replaced. 
