package simpledb.test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
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
 * @author Chirag
 */
@SuppressWarnings("static-access")
public class TestBuffer {

	private static Registry reg;
	private static final int DEFAULT_PORT = 1099;
	private static final String BINDING_NAME = "simpledb";

	/**
	 * Setting up the resources to be used by the test cases. This method runs once
	 * before running the test cases.
	 * 
	 * @throws RemoteException
	 *             If some error occurs while binding the registry.
	 */
	@BeforeClass
	public static void setUp() throws RemoteException {
		System.out.println(
				"----------------------------------------Test Buffer Setting up----------------------------------------");
		SimpleDB.init("simpleDB");
		// create a registry specific for the server on the default port
		reg = LocateRegistry.createRegistry(DEFAULT_PORT);
		// and post the server entry in it
		reg.rebind(BINDING_NAME, new RemoteDriverImpl());
		System.out.println(
				"----------------------------------------Test Buffer Set Up done----------------------------------------");
	}

	/**
	 * Releasing the resources to be used by the test cases. This method runs once
	 * after running all the test cases
	 * 
	 * @throws RemoteException
	 *             If some error occurs while un-binding the registry.
	 * @throws NotBoundException
	 *             If some error occurs while un-binding the registry.
	 */
	@AfterClass
	public static void tearDown() throws RemoteException, NotBoundException {
		System.out.println(
				"----------------------------------------Test Buffer Tearing down----------------------------------------");
		reg.unbind(BINDING_NAME);
		reg = null;
		System.out.println(
				"----------------------------------------Test Buffer Tear down done----------------------------------------");
	}

	/**
	 * <h2>Test Scenario 1:</h2>
	 * <p>
	 * Pin 8 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5, 6, 7</b>
	 * </p>
	 * <p>
	 * Unpin 2 blocks in the following order:
	 * </p>
	 * <p>
	 * <b>2, 1</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, now amongst the unpinned blocks, 1 is the
	 * least recently used block, it will be replaced.<br>
	 * This is asserted by using checking that block 1 is not present in the buffer
	 * pool and block 8 is present in the buffer pool.
	 * </p>
	 * 
	 */
	@Test
	public void testScenario1() {
		System.out.println("----------Running Buffer Test Scenario 1----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		System.out.println("Creating 9 Blocks");
		Block[] blocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 8; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Buffer Pool after setting 8 blocks:");
		printBufferPool(basicBufferMgr);

		System.out.println("Unpining Blocks");
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning blocks 2 and 1:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning new Block 8");
		basicBufferMgr.pin(blocks[8]);
		System.out
				.println("Now we have 2 unpinned buffers available. As per LRU(2) Policy, buffer 1 will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(basicBufferMgr);

		Assert.assertFalse(basicBufferMgr.getBufferPoolMap().containsKey(blocks[1]));
		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[8]));
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 1 Run Complete----------");
	}

	/**
	 * <h2>Test Scenario 2:</h2>
	 * <p>
	 * Pin 8 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5, 6, 7</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, Now since there is no unpinned block in the
	 * buffer pool, it will throw a BufferAbortException. This is asserted by
	 * checking inside the catch block for BufferAbortException. <br>
	 * The Failing case for this test is the line: <b>Assert.assertTrue(false)</b>
	 * after it tries to pin the 9the block to a filled buffer. The assert statement
	 * will evaluate to false and fail the test case indicating that the
	 * BufferAbortException was not thrown as expected.<br>
	 * </p>
	 * 
	 */
	@Test
	public void testScenario2() {
		System.out.println("----------Running Buffer Test Scenario 2----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();

		System.out.println("Creating 9 Blocks");
		Block[] blocks = new Block[9];
		for (int i = 0; i < 9; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 8; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Buffer Pool after setting 8 blocks:");
		printBufferPool(basicBufferMgr);

		try {
			System.out.println("Pinning new Block 8");
			basicBufferMgr.pin(blocks[8]);
			Assert.assertTrue(false); // If pin does not throw an exception then
										// test case is failed.
		} catch (BufferAbortException e) {
			System.out.println("Exception Thrown: " + e.getClass());
			Assert.assertTrue(true); // If pin throws an exception then test
										// case is passed.
		}

		System.out.println("Buffer Pool after trying to pin new block 8:");
		printBufferPool(basicBufferMgr);
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 2 Run Complete----------");
	}

	/**
	 * <h2>Test Scenario 3:</h2>
	 * <p>
	 * Pin 6 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5</b>
	 * </p>
	 * <p>
	 * Unpin 2 blocks in the following order:
	 * </p>
	 * <p>
	 * <b>2, 1</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, Now even though Block 1 is the least recently
	 * used unpinned block, we still have available space in buffer pool so, no
	 * replacement This is asserted by checking that block 1 and block 8 both are
	 * present in the buffer pool.
	 * </p>
	 * 
	 */
	@Test
	public void testScenario3() {
		System.out.println("----------Running Buffer Test Scenario 3----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();

		System.out.println("Creating 10 Blocks");
		Block[] blocks = new Block[10];
		for (int i = 0; i < 10; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 6; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Buffer Pool after setting 6 blocks:");
		printBufferPool(basicBufferMgr);

		System.out.println("Unpining Blocks");
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning blocks 2 and 1:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning new Block 8");
		basicBufferMgr.pin(blocks[8]);
		System.out.println("Now we have 2 unpinned buffers available. As per replacement Policy, the earlier unpinned buffer will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(basicBufferMgr);

		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[1]));
		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[8]));
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 3 Run Complete----------");
	}

	/**
	 * <h2>Test Scenario 4:</h2>
	 * <p>
	 * Pin 8 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5, 6, 7</b>
	 * </p>
	 * <p>
	 * Pin Block 1 again.<br>
	 * Unpin 2 blocks in the following order:
	 * </p>
	 * <p>
	 * <b>2, 1</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, Now Block 1 was pinned twice but unpinned
	 * only once, so it will not be replaced and block 2 will be replaced
	 * instead.<br>
	 * This is asserted by checking that block 1 and block 8 are present in the
	 * buffer pool and block 2 is not present in the buffer pool.
	 * </p>
	 * 
	 */
	@Test
	public void testScenario4() {
		System.out.println("----------Running Buffer Test Scenario 4----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();

		System.out.println("Creating 10 Blocks");
		Block[] blocks = new Block[10];
		for (int i = 0; i < 10; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 8; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Pinning Block 1 Again");
		basicBufferMgr.pin(blocks[1]);

		System.out.println("Buffer Pool after setting 8 blocks:");
		printBufferPool(basicBufferMgr);

		System.out.println("Unpining Blocks");
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning blocks 2 and 1:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning new Block 8");
		basicBufferMgr.pin(blocks[8]);
		System.out.println("Now we have 1 unpinned buffers available. Since block 1 was pinned twice and unpinned only once.\nSo, Block 2 will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(basicBufferMgr);

		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[1]));
		Assert.assertFalse(basicBufferMgr.getBufferPoolMap().containsKey(blocks[2]));
		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[8]));
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 4 Run Complete----------");
	}

	/**
	 * <h2>Test Scenario 5:</h2>
	 * <p>
	 * Pin 8 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5, 6, 7</b>
	 * </p>
	 * <p>
	 * Pin Block 1 and 2 again.<br>
	 * Unpin 2 blocks in the following order:
	 * </p>
	 * <p>
	 * <b>2, 1, 2, 1</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, Now Block 1 and 2 were pinned twice and
	 * unpinned twice but block 1 was k-pinned before 2 so it will be replaced and
	 * block 2 will be not be replaced.<br>
	 * This is asserted by checking that block 2 and block 8 are present in the
	 * buffer pool and block 1 is not present in the buffer pool.
	 * </p>
	 * 
	 */
	@Test
	public void testScenario5() {
		System.out.println("----------Running Buffer Test Scenario 5----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();

		System.out.println("Creating 10 Blocks");
		Block[] blocks = new Block[10];
		for (int i = 0; i < 10; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 8; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Pinning Block 1 Again");
		basicBufferMgr.pin(blocks[1]);
		System.out.println("Pinning Block 2 Again");
		basicBufferMgr.pin(blocks[2]);

		System.out.println("Buffer Pool after setting 8 blocks:");
		printBufferPool(basicBufferMgr);

		System.out.println("Unpining Blocks");
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning blocks 2 and 1:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning new Block 8");
		basicBufferMgr.pin(blocks[8]);
		System.out.println(
				"Now we have 2 unpinned buffers available. Since block 1 was pinned before Block 2, it will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(basicBufferMgr);

		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[2]));
		Assert.assertFalse(basicBufferMgr.getBufferPoolMap().containsKey(blocks[1]));
		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[8]));
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 5 Run Complete----------");
	}

	/**
	 * <h2>Test Scenario 6:</h2>
	 * <p>
	 * Pin 8 blocks to buffer pool of default size (8), In the following order:
	 * </p>
	 * <p>
	 * <b>0, 1, 2, 3, 4, 5, 6, 7</b>
	 * </p>
	 * <p>
	 * Pin Block 1.<br>
	 * Unpin 2 blocks in the following order:
	 * </p>
	 * <p>
	 * <b>2, 1, 1</b>
	 * </p>
	 * <p>
	 * Pin a new block to buffer pool, Now Block 1 was pinned twice and unpinned twice
	 * Block 2 was pinned once and unpinned once 
	 * Block 2 has a k distance of infinity so it will be replaced and
	 * block 1 will be not be replaced.<br>
	 * This is asserted by checking that block 1 and block 8 are present in the
	 * buffer pool and block 2 is not present in the buffer pool.
	 * </p>
	 * 
	 */
	@Test
	public void testScenario6() {
		System.out.println("----------Running Buffer Test Scenario 5----------");
		BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();

		System.out.println("Creating 10 Blocks");
		Block[] blocks = new Block[10];
		for (int i = 0; i < 10; i++) {
			System.out.println("\tCreating Block " + (i + 1));
			blocks[i] = new Block("filename", i);
		}

		System.out.println("Initial State of Buffer Pool:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning the Blocks");
		Buffer[] buffers = new Buffer[8];
		for (int i = 0; i < 8; i++) {
			Block blk = blocks[i];
			System.out.println("\tPinning Block " + blk);
			Buffer buf = basicBufferMgr.pin(blk);
			System.out.println("\tBlock Pinned to Buffer " + buf);
			buffers[i] = buf;
		}

		System.out.println("Pinning Block 1 Again");
		basicBufferMgr.pin(blocks[1]);

		System.out.println("Buffer Pool after setting 8 blocks:");
		printBufferPool(basicBufferMgr);

		System.out.println("Unpining Blocks");
		System.out.println("\tUnpining Block 2");
		basicBufferMgr.unpin(buffers[2]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);
		System.out.println("\tUnpining Block 1");
		basicBufferMgr.unpin(buffers[1]);

		System.out.println("Buffer Pool after unpinning blocks 2 and 1:");
		printBufferPool(basicBufferMgr);

		System.out.println("Pinning new Block 8");
		basicBufferMgr.pin(blocks[8]);
		System.out.println("Now we have 2 unpinned buffers available. Since block 2 has back distance infinity Block 2 will be replaced.");

		System.out.println("Buffer Pool after pinning new block 8:");
		printBufferPool(basicBufferMgr);

		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[1]));
		Assert.assertFalse(basicBufferMgr.getBufferPoolMap().containsKey(blocks[2]));
		Assert.assertTrue(basicBufferMgr.getBufferPoolMap().containsKey(blocks[8]));
		basicBufferMgr.getBufferPoolMap().clear();
		basicBufferMgr.resetNumAvailable();
		basicBufferMgr.resetCounter();
		System.out.println("----------Buffer Test Scenario 6 Run Complete----------");
	}

	/**
	 * Method to print the contents of buffer pool.
	 * 
	 * @param basicBufferMgr
	 *            the buffer manager through which the buffer pool is to be printed.
	 */
	private void printBufferPool(BufferMgr basicBufferMgr) {
		int i = 0;
		if (basicBufferMgr.getBufferPoolMap().size() == 0) {
			System.out.println("Buffer pool is empty");
		}
		for (HashMap.Entry<Block, Buffer> e : basicBufferMgr.getBufferPoolMap().entrySet()) {
			System.out.println("\t" + ++i + "): " + e.getKey().toString() + " = [" + e.getValue().toString() + "]\t, l="
					+ e.getValue().getLastAccess() + "\t,s=" + e.getValue().getSecondLastAccess());
		}
	}

}
