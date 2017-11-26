package simpledb.buffer;

import java.util.HashMap;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	/**
	 * Added HashMap to keep track of buffers in the pool instead of an array because retrieval is faster
	 * Created a counter variable to track the 'pin time' of a block Created a
	 * bufferPoolSize to store the maximum limit on the buffer pool size
	 * 
	 * @author Team number xxx
	 */
	private HashMap<Block, Buffer> bufferPoolMap;
	private int numAvailable;
	private int bufferPoolSize;
	public static double counter;

	/**
	 * Creates a buffer manager having the specified number of buffer slots. This
	 * constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 * 
	 * Removed array initialization and replaced it with HashMap.
	 * Assigning bufferPoolSize
	 * 
	 * @author Team number xxx
	 */
	BasicBufferMgr(int numbuffs) {
		bufferPoolMap = new HashMap<Block, Buffer>();
		numAvailable = numbuffs;
		bufferPoolSize = numbuffs;
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferPoolMap.values())
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer assigned
	 * to that block then that buffer is used; otherwise, an unpinned buffer from
	 * the pool is chosen. Returns a null value if there are no available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 * 
	 * Check if the block is there in the buffer pool by calling findExistingBuffer.
	 * Choose a victim/new buffer if block is not present in pool and remove it from pool
	 * Assign the block to the buffer above
	 * Increment the counter to keep track of when the block was pinned
	 * Put the buffer in the buffer pool (HashMap)
	 * 
	 * @author Team number xxx
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			bufferPoolMap.remove(buff.block());
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		counter++;
		buff.pin();
		bufferPoolMap.put(blk, buff);
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it. Returns
	 * null (without allocating the block) if there are no available buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 * 
	 * Choose a victim/new buffer if block is not present in pool and remove it from pool
	 * Assign the block to the buffer above
	 * Increment the counter to keep track of when the block was pinned
	 * Put the buffer in the buffer pool (HashMap)
	 * 
	 * @author Team number xxx
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		bufferPoolMap.remove(buff.block());
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		counter++;
		buff.pin();
		bufferPoolMap.put(buff.block(), buff);
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}
	
	/**
	 * Find and return the block from the HashMap instead of array else null
	 * 
	 * @author Team number xxx
	 */
	private Buffer findExistingBuffer(Block blk) {
		if (bufferPoolMap.containsKey(blk)) {
			return bufferPoolMap.get(blk);
		} else {
			return null;
		}
	}

	private Buffer chooseUnpinnedBuffer() {
		if (numAvailable > 0) {
			if (bufferPoolMap.size() < bufferPoolSize) {
				Buffer buff = new Buffer();
				return buff;
			} else {
				Buffer lruBuff = null;
				for (Buffer b : bufferPoolMap.values()) {
					if (!b.isPinned()) {
						System.out.println("Considering buffer " + b.getLastAccess() + ", " + b.getSecondLastAccess());
						if (lruBuff == null) {
							lruBuff = b;
						} else {
							
							// If both the current block and replacement candidate was pinned twice or more
							if (b.getSecondLastAccess() != 0 && lruBuff.getSecondLastAccess() != 0) {
								if (b.getLastAccess() < lruBuff.getLastAccess()) {
									lruBuff = b;
								}
							}
							// If both the current block and replacement candidate was pinned only once
							if (b.getSecondLastAccess() == 0 && lruBuff.getSecondLastAccess() == 0) {
								if (b.getLastAccess() < lruBuff.getLastAccess()) {
									lruBuff = b;
								}
							}/*
							// If the current block was pinned only once and replacement candidate was pinned twice or more
							if (b.getSecondLastAccess() == 0 && lruBuff.getSecondLastAccess() != 0) {
								if (b.getLastAccess() < lruBuff.getSecondLastAccess()) {
									lruBuff = b;
								}
							}*/
							// If the current block was pinned twice and replacement candidate was pinned only once
							if (b.getSecondLastAccess() == 0 || lruBuff.getSecondLastAccess() == 0) {
								lruBuff = b.getSecondLastAccess() != 0 ? lruBuff: b;
							}
						}
					}
				}

				System.out.println("returning buffer " + lruBuff.getLastAccess() + ", " + lruBuff.getSecondLastAccess());
				lruBuff.setLastAccess(0);
				lruBuff.setSecondLastAccess(0);
				return lruBuff;
			}
		}
		throw new BufferAbortException();
	}

	/**
	 * Determines whether the map has a mapping from the block to some buffer.
	 * 
	 * @paramblk the block to use as a key
	 * @return true if there is a mapping; false otherwise
	 */
	boolean containsMapping(Block blk) {
		return bufferPoolMap.containsKey(blk);
	}

	/**
	 * Returns the buffer that the map maps the specified block to.
	 * 
	 * @paramblk the block to use as a key
	 * @return the buffer mapped to if there is a mapping; null otherwise
	 */
	Buffer getMapping(Block blk) {
		return bufferPoolMap.get(blk);
	}

	/**
	 * Methods to help run JUnit Test cases
	 * 
	 * @return the bufferPoolMap
	 */
	public HashMap<Block, Buffer> getBufferPoolMap() {
		return bufferPoolMap;
	}

	/**
	 * Methods to help run JUnit Test cases resets the numAvailable count
	 */
	public void resetNumAvailable() {
		numAvailable = bufferPoolSize;
	}
}
