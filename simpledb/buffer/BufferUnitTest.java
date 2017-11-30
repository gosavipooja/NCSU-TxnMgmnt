package simpledb.buffer;

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import simpledb.file.Block;
import simpledb.server.SimpleDB;

/**
 * JUnit test cases for Buffer Pool management using LRU(2)
 * @author Team F
 *
 */
public class BufferUnitTest {
	
	private HashMap<Integer,Block> blocksMap = new HashMap<>();
	private BufferMgr myBufferMgr;
	
	private HashMap<Integer, Block> beforeMap;
	private HashMap<Integer, Block> afterMap;
	
	private static String dbname = "studentdb";
	private static String filename = "filename";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		
		/* Create new buffer manager for every test */
		SimpleDB.initFileLogAndBufferMgr(dbname);
		myBufferMgr = SimpleDB.bufferMgr();
		
		/* Initialize 10 blocks for testing from block 11 to 20 */
		for (int i=0; i<10; i++) {
			blocksMap.put(i+11,new Block(filename, i+11));
		}
		
		/*
		 * Pin the first 7 blocks
		 * One buffer is already used by log manager
		 */
		try {
			for (int i=11; i<=17; i++) {
				myBufferMgr.pin(blocksMap.get(i));
			}
		} catch(BufferAbortException e) {
			System.out.println(e.getMessage());
		}
			
		/* Cache the buffer pool map before the pages were replaced */
		beforeMap = getMap(myBufferMgr);
		
//		System.out.println("\nBuffer pool state BEFORE replacement:");
//		dumpBufferPool(myBufferMgr);
		
	}

	@After
	public void tearDown() throws Exception {
		/* Cache the buffer pool map after the pages were replaced */
		afterMap = getMap(myBufferMgr);
		
//		System.out.println("\nBuffer pool state AFTER replacement:");
//		dumpBufferPool(myBufferMgr);
		
		System.out.println("\nChanges in Buffer Pool:");
		diffMap(beforeMap,afterMap);
	}

	@Test
	public void basicReplacementTest1() {
		System.out.println("\n\nIn Basic Replacement Test 1...");
		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = myBufferMgr.getBufferPoolMap();
		
		Block blk15 = blocksMap.get(15);
		int blk15_bufId = myBufferMgr.getMapping(blk15).getBufferId();
		Block blk18 = blocksMap.get(18);
		Block blk19 = blocksMap.get(19);
		Block blk20 = blocksMap.get(20);

		System.out.println("\nEvent(s):");
		
		myBufferMgr.unpin(myBufferMgr.getMapping(blk15));
		
		
		myBufferMgr.pin(blk18);
		
		/* Block 15 is replaced
		 * Block 18 is present
		 * Buffer ID of 18 is same as that of 15
		 * */
		boolean testCondition = !(myBufferMgr.containsMapping(blk15)) && myBufferMgr.containsMapping(blk18) && (blk15_bufId == myBufferMgr.getMapping(blk18).getBufferId());
		
		assertTrue(testCondition);
		
	}
	
	
	@Test
	public void basicReplacementTest2() {
		System.out.println("\n\nIn Basic Replacement Test 2...");
		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = myBufferMgr.getBufferPoolMap();
		
		Block blk13 = blocksMap.get(13);
		int blk13_bufId = myBufferMgr.getMapping(blk13).getBufferId();
		Block blk15 = blocksMap.get(15);
		int blk15_bufId = myBufferMgr.getMapping(blk15).getBufferId();
		Block blk17 = blocksMap.get(17);
		int blk17_bufId = myBufferMgr.getMapping(blk17).getBufferId();
		Block blk18 = blocksMap.get(18);
		Block blk19 = blocksMap.get(19);
		Block blk20 = blocksMap.get(20);

		System.out.println("\nEvent(s):");
		

		
		myBufferMgr.unpin(myBufferMgr.getMapping(blk17));
		
		
		myBufferMgr.unpin(myBufferMgr.getMapping(blk15));
		
		
		myBufferMgr.unpin(myBufferMgr.getMapping(blk13));
		
		
		myBufferMgr.pin(blk18);
		
		 /*Block 13 is replaced
		 Block 15 is NOT replaced
		 Block 17 is NOT replaced
		 Block 18 is present
		 Buffer ID of 18 is same as that of 15
		 */
		boolean testCondition =  (myBufferMgr.containsMapping(blk15)) && myBufferMgr.containsMapping(blk17) && !myBufferMgr.containsMapping(blk13) && myBufferMgr.containsMapping(blk18) && (blk13_bufId == myBufferMgr.getMapping(blk18).getBufferId());
		assertTrue(testCondition);
		
	}
	
	
	
	@Test
	public void lru2ReplacementTest1() {
		System.out.println("\n\nIn LRU2 Replacement Test 1...");
		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = myBufferMgr.getBufferPoolMap();
		
		Block blk13 = blocksMap.get(13);
		int blk13_bufId = myBufferMgr.getMapping(blk13).getBufferId();
		Block blk15 = blocksMap.get(15);
		int blk15_bufId = myBufferMgr.getMapping(blk15).getBufferId();
		Block blk17 = blocksMap.get(17);
		int blk17_bufId = myBufferMgr.getMapping(blk17).getBufferId();
		Block blk18 = blocksMap.get(18);
		Block blk19 = blocksMap.get(19);
		Block blk20 = blocksMap.get(20);

		System.out.println("\nEvent(s):");
		
		
		myBufferMgr.pin(blk17);
		
		
		myBufferMgr.pin(blk15);
		
		
		myBufferMgr.pin(blk13);
		
		
		for(int i=0; i<2; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk17));
		
		
		for(int i=0; i<2; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk15));
		
		
		for(int i=0; i<2; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk13));
		
		
		
		myBufferMgr.pin(blk18);
		
		 /*Block 13 is replaced
		 Block 15 is NOT replaced
		 Block 17 is NOT replaced
		 Block 18 is present
		 Buffer ID of 18 is same as that of 15*/
		boolean testCondition = !(myBufferMgr.containsMapping(blk13)) && myBufferMgr.containsMapping(blk15) && myBufferMgr.containsMapping(blk17) && myBufferMgr.containsMapping(blk18) && (blk13_bufId == myBufferMgr.getMapping(blk18).getBufferId());
		assertTrue(testCondition);
		
	}
	
	

	@Test
	public void lru2ReplacementTest2() {
		System.out.println("\n\nIn LRU2 Replacement Test 2...");
		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = myBufferMgr.getBufferPoolMap();
		
		Block blk13 = blocksMap.get(13);
		int blk13_bufId = myBufferMgr.getMapping(blk13).getBufferId();
		Block blk15 = blocksMap.get(15);
		int blk15_bufId = myBufferMgr.getMapping(blk15).getBufferId();
		Block blk17 = blocksMap.get(17);
		int blk17_bufId = myBufferMgr.getMapping(blk17).getBufferId();
		Block blk18 = blocksMap.get(18);
		Block blk19 = blocksMap.get(19);
		Block blk20 = blocksMap.get(20);
		

		System.out.println("\nEvent(s):");
		
		
		for(int i=0; i<3; i++)
			myBufferMgr.pin(blk17);
		
		
		for(int i=0; i<3; i++)
			myBufferMgr.pin(blk15);

		
		for(int i=0; i<3; i++)
			myBufferMgr.pin(blk13);
		
		
		for(int i=0; i<4; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk17));
		
		
		for(int i=0; i<4; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk15));
		
		
		for(int i=0; i<4; i++)
			myBufferMgr.unpin(myBufferMgr.getMapping(blk13));
		
		
		
		myBufferMgr.pin(blk18);
		
		
		myBufferMgr.pin(blk19);
		
		/* Block 15 is replaced
		 Block 17 is replaced
		 Block 18 is present
		 Block 19 is present
		 Buffer ID of 18 is same as that of 17
		 Buffer ID of 19 is same as that of 15*/
		boolean testCondition = !(myBufferMgr.containsMapping(blk15)) && !(myBufferMgr.containsMapping(blk17)) && myBufferMgr.containsMapping(blk18) &&
				myBufferMgr.containsMapping(blk19) && (blk17_bufId == myBufferMgr.getMapping(blk18).getBufferId()) && (blk15_bufId == myBufferMgr.getMapping(blk19).getBufferId());
		assertTrue(testCondition);
	}
	
	
	
	@Test
	public void masterLruTest() {
		System.out.println("\n\nIn Master LRU test...");

		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = myBufferMgr.getBufferPoolMap();
		
		Block blk15 = blocksMap.get(15);
		int blk15_bufId = myBufferMgr.getMapping(blk15).getBufferId();
		Block blk17 = blocksMap.get(17);
		int blk17_bufId = myBufferMgr.getMapping(blk17).getBufferId();
		Block blk18 = blocksMap.get(18);
		Block blk19 = blocksMap.get(19);
		Block blk20 = blocksMap.get(20);
		
		int pinOrder[] = {14,12,17,11};
		int unpinOrder[] = {17,16,15,14,11,17,14,12,12};
		
		System.out.println("\nEvent(s):");
		
		// Pin the blocks
		for(int p = 0 ; p< pinOrder.length; p++) {
			int i = pinOrder[p];
			
			myBufferMgr.pin(blocksMap.get(i));
		}

		// Unpin the blocks
		for(int p =0 ; p< unpinOrder.length; p++) {
			int i = unpinOrder[p];
			Buffer buf = myBufferMgr.getMapping(blocksMap.get(i));
			
			myBufferMgr.unpin( buf );
		}
		
		
		myBufferMgr.pin(blk19);
		
		 /*Block 15 is replaced
		 Block 19 is present
		 Buffer ID of 19 is same as that of 15*/
		boolean testCondition = !(myBufferMgr.containsMapping(blk15)) && myBufferMgr.containsMapping(blk19) && (blk15_bufId == myBufferMgr.getMapping(blk19).getBufferId());
		assertTrue(testCondition);
	}
	
	
	
	private static void dumpBufferPool(BufferMgr buffMgr) {
		
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = buffMgr.getBufferPoolMap();
		
		/* Sort the buffer pool map based on buffer ID */
		TreeSet<Buffer> sortedBufferPool = new TreeSet<>(new Comparator<Buffer>() {
			@Override
			public int compare(Buffer b1, Buffer b2) {
				return b1.getBufferId()-b2.getBufferId();
			}
		});
		
		for(Block b: buffPoolMap.keySet()) {
			sortedBufferPool.add(buffPoolMap.get(b));
		}
		
		for(Buffer b: sortedBufferPool) {
			double l = b.getlastPin();
			double s = b.getSecondLastPin();
			System.out.println("[ Buffer "+b.getBufferId()+" ] : Block "+b.block()+" [l="+l+", s="+s+"] Pinned="+b.isPinned());
		}
	}
	
	/**
	 * returns Buffer ID -> Block map
	 */
	private static HashMap<Integer,Block> getMap(BufferMgr buffMgr) {
		/* Retrieve the buffer pool map */
		HashMap<Block,Buffer> buffPoolMap = buffMgr.getBufferPoolMap();
		
		HashMap<Integer,Block> result = new HashMap<>();
		
		for(Block b: buffPoolMap.keySet()) {
			Buffer buf = buffPoolMap.get(b);
			result.put(buf.getBufferId(), b);
		}
		
		return result;
		
	}
	
	private static void diffMap(HashMap<Integer,Block> map1,
								HashMap<Integer,Block> map2) {
		
		for(Integer k:map1.keySet()) {
			Block b1 = map1.get(k);
			Block b2 = map2.get(k);
			
			if(b1!=b2) {
				System.out.println("Buff "+k+" : Block "+b1+" replaced by Block "+b2);
			}
		}
	}
}
