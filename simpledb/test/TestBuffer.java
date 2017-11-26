package simpledb.test;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.RemoteDriverImpl;
import simpledb.server.SimpleDB;

/**
 * @author Team F
 */
@SuppressWarnings("static-access")
public class TestBuffer {

	private static Registry registry;
	private static final int DEFAULT_PORT = 1099;
	private static final String BINDING_NAME = "simpledb";

	/**
	 * Environment setup before Test cases. 
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		System.out.println("---------------Test Environment Setup Started---------------------------\n");
		SimpleDB.init("simpleDB");
		registry = LocateRegistry.createRegistry(DEFAULT_PORT);
		registry.rebind(BINDING_NAME, new RemoteDriverImpl());
		System.out.println("--------------Test Environment Setup Completed--------------------------\n");
	}

	/**
	 * Environment cleanup after Test cases. 
	 */
	@AfterClass
	public static void tearDown() throws Exception {
		System.out.println("---------------Test Environment Cleanup Started---------------------------\n");
		registry.unbind(BINDING_NAME);
		registry = null;
		System.out.println("--------------Test Environment Cleanup Completed--------------------------\n");
	}

	/**
	 * Case 1: Fill the buffer pool, unpin to create space, pin new block 
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order: 
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 * 
	 * Unpin 2 testBlocks in the following order: 
	 * 2, 1
	 * 
	 * Pin a new block to buffer pool. Now amongst the unpinned testBlocks, 1 is the
	 * least recently used block, it will be replaced. 
	 * 
	 * This is checked in the following way.
	 * 
	 * block 1 is not present in the buffer pool. 
	 * block 8 is present in the buffer pool.
	 * 
	 */
	
	@Test
	public void testCase1() {
		System.out.println("----------Start Test Case #1----------\n");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		try {
			testBBMgr.pin(testBlocks[8]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		
		System.out.println("There are 2 unpinned buffers available. Buffer 1 will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #1----------\n");
	}

	/**
	 * Case 2: Fill the buffer pool, try to pin new buffer
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 * 
	 * Pin a new block to buffer pool, Now there is no unpinned block in the
	 * buffer pool or empty buffer it will throw a BufferAbortException. 
	 * 
	 * This is checked in the following way.
	 * 
	 * Checking inside the catch block for BufferAbortException. 
	 * 
	 */
	@Test
	public void testCase2() {
		System.out.println("----------Start Test Case #2----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		try {
			System.out.println("Pinning new Block 8");
			testBBMgr.pin(testBlocks[8]);
			Assert.assertTrue(false); 
		} catch (BufferAbortException e) {
			System.out.println("Exception " + e.getClass());
			Assert.assertTrue(true);
		}

		System.out.println("Buffer Pool after trying to pin new block 8:");
		printBufferPool(testBBMgr);
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #2----------");
	}

	/**
	 * Case 3: Partially fill buffer pool, unpin, on new pin empty buffer should be used
	 * 
	 * Pin 6 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 1
	 * 
	 * Pin a new block to buffer pool, Even though Block 1 is the least recently
	 * used unpinned block, we still have available space in buffer pool so, no
	 * replacement 
	 * 
	 * This is checked in the following way.
	 * 
	 * Checking that block 1 and block 8 both are present in the buffer pool.
	 *  
	 */
	@Test
	public void testCase3() {
		System.out.println("----------Start Test Case #3----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {	
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[6];
		try {
			for (int i = 0; i < 6; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 6 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		try {
			testBBMgr.pin(testBlocks[8]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		
		System.out.println("There are 2 unpinned buffers available and also empty buffer, so there should be no replacement.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #3----------");
	}

	/**
	 * Case 4: Fill the buffer pool, Check if pinned block is not replaced
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 *  
	 * Pin Block 1 again. 
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 1
	 * 
	 * Pin a new block to buffer pool
	 * Block 1 was pinned twice but unpinned only once
	 * So it will not be replaced and block 2 will be replaced instead.
	 * 
	 * This is checked in the following way.
	 * 
	 * Block 1 and Block 8 are present in the buffer pool
	 * Block 2 is not present in the buffer pool.
	 * 
	 */
	@Test
	public void testCase4() {
		System.out.println("----------Start Test Case #4----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Pinning Block 1 Again");
		try {
			testBBMgr.pin(testBlocks[1]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		
		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		try {
			testBBMgr.pin(testBlocks[8]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		System.out.println("There is 1 unpinned buffers available because block 1 was pinned twice and unpinned only once. So, Block 2 will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[2]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #4----------");
	}

	/**
	 * Case 5: Fill the buffer pool, Pin and Unpin two blocks. Check if the LRU block is replaced. 
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 *  
	 * Pin Block 1 and 2 again. 
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 1, 2, 1
	 *  
	 * Pin a new block to buffer pool
	 * 
	 * Block 1 and 2 were pinned twice and unpinned twice
     * block 1 has larger back distance compared to block 2 so it will be replaced 
     * 
	 * This is checked in the following way.
	 * 
	 * block 2 and block 8 are present in the buffer pool
	 * block 1 is not present in the buffer pool.
	 * 
	 */
	@Test
	public void testCase5() {
		System.out.println("----------Start Test Case #5----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		try {
			System.out.println("Pinning Block 1 Again");
			testBBMgr.pin(testBlocks[1]);
			System.out.println("Pinning Block 2 Again");
			testBBMgr.pin(testBlocks[2]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		try {
			testBBMgr.pin(testBlocks[8]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		
		System.out.println("There are 2 unpinned buffers available. Since block 1 was pinned before Block 2, it will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[2]));
		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #5----------");
	}

	/**
	 * Case 6: Fill the buffer pool, Check if the block that is only pinned once is replaced
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 * 
	 * 
	 * Pin Block 1. 
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 1, 1
	 * 
	 * Pin a new block to buffer pool, 
	 * Block 1 was pinned twice and unpinned twice 
	 * Block 2 was pinned once and unpinned once Block 2 has a k distance of infinity 
	 * so it will be replaced and block 1 will be not be replaced. 
	 * 
	 * This is checked in the following way.
	 * 
	 * block 1 and block 8 are present in the buffer pool
	 * block 2 is not present in the buffer pool.
	 * 
	 */
	@Test
	public void testCase6() {
		System.out.println("----------Start Test Case #6----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Pinning Block 1 Again");
		try {
			testBBMgr.pin(testBlocks[1]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		try {
			testBBMgr.pin(testBlocks[8]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}
		System.out.println("There are 2 unpinned buffers. Since block 2 has back distance infinity it will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[2]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #6----------");
	}

	/**
	 * Case 7: Fill the buffer pool, Check if the block that is only pinned once is replaced
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 * 
	 * Pin Block 2. 
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 2, 1
	 * 
	 * 
	 * Pin a new block to buffer pool,
	 * Block 2 was pinned twice and unpinned twice 
	 * Block 1 was pinned once and unpinned once 
	 * Block 1 has a k distance of infinity and also was pinned before 2 
	 * so it will be replaced and block 2 will be not be replaced. 
	 * 
	 * This is checked in the following way. 
	 * 
	 * block 2 and block 8 are present in the buffer pool 
	 * block 1 is not present in the buffer pool.
	 * 
	 */
	@Test
	public void testCase7() {
		System.out.println("----------Start Test Case #7----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Pinning Block 2 Again");
		try {
			testBBMgr.pin(testBlocks[2]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		testBBMgr.pin(testBlocks[8]);
		System.out.println("There are 2 unpinned buffers. Since block 1 has back distance infinity and also was pinned before block 2 it will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[2]));
		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #7----------");
	}

	/**
	 * Case 8: Larger back distance buffer is replaced
	 * 
	 * Pin 8 testBlocks to buffer pool in the following order:
	 * 0, 1, 2, 3, 4, 5, 6, 7
	 * 
	 * Pin Block 2 three time. 
	 * Pin block 1 
	 * 
	 * Unpin 2 testBlocks in the following order:
	 * 2, 2, 2, 2, 1, 1
	 *  
	 * Pin a new block to buffer pool 
	 * Block 1 was pinned last, however the back distance of block 1 is larger than block 2 thus block 2 will be not be replaced.
	 * 
	 * This is checked in the following way. 
	 * block 2 and block 8 are present in the buffer pool 
	 * block 1 is not present in the buffer pool.
	 * 
	 */
	@Test
	public void testCase8() {
		System.out.println("----------Start Test Case #8----------");
		BufferMgr testBBMgr = new SimpleDB().bufferMgr();
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetCounter();
		System.out.println("Test testBlocks creation started");
		Block[] testBlocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			testBlocks[i] = new Block("filename", i);
		}
		System.out.println("Test testBlocks creation completed");

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(testBBMgr);

		System.out.println("Block Pinning Started");
		Buffer[] buffers = new Buffer[8];
		try {
			for (int i = 0; i < 8; i++) {
				Block blk = testBlocks[i];
				System.out.println("  Pinning Block: " + blk);
				Buffer buf = testBBMgr.pin(blk);
				buffers[i] = buf;
			}		
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		try {
			System.out.println("Pinning Block 2 Again");
			testBBMgr.pin(testBlocks[2]);
			System.out.println("Pinning Block 2 Again");
			testBBMgr.pin(testBlocks[2]);
			System.out.println("Pinning Block 2 Again");
			testBBMgr.pin(testBlocks[2]);
			System.out.println("Pinning Block 1 Again");
			testBBMgr.pin(testBlocks[1]);
		} catch (BufferAbortException e) {
			System.out.println("Exception occured while Pinning BLock");
		}

		System.out.println("Buffer Pool after pinning 8 testBlocks:");
		printBufferPool(testBBMgr);

		System.out.println("Unpining testBlocks");
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 2");
		testBBMgr.unpin(buffers[2]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);
		System.out.println("  Unpinning Block 1");
		testBBMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning testBlocks 2 and 1:");
		printBufferPool(testBBMgr);

		System.out.println("Pinning new Block 8");
		testBBMgr.pin(testBlocks[8]);
		System.out.println("There are 2 unpinned buffers. Block 1 has largest back distance even though it was pinned recently it will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(testBBMgr);

		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[2]));
		Assert.assertFalse(testBBMgr.getBufferPoolMap().containsKey(testBlocks[1]));
		Assert.assertTrue(testBBMgr.getBufferPoolMap().containsKey(testBlocks[8]));
		testBBMgr.getBufferPoolMap().clear();
		testBBMgr.resetNumAvailable();
		testBBMgr.resetCounter();
		System.out.println("----------End Test Case #8----------");
	}

	/**
	 * Method to print the contents of buffer pool.
	 * 
	 * @param testBBMgr
	 *            the buffer manager.
	 */
	private void printBufferPool(BufferMgr testBBMgr) {
		if (testBBMgr.getBufferPoolMap().size() == 0) {
			System.out.println("Empty buffer pool");
		} else {
			int i = 1;
			for (HashMap.Entry<Block, Buffer> e : testBBMgr.getBufferPoolMap().entrySet()) {
				System.out.println(" " + i + " --> " + e.getKey().toString() + 
						"\t[l=" + e.getValue().getlastPin() + 
						" ,s=" + e.getValue().getSecondLastPin() + "]");
				i++;
			}
		}
	}

}
