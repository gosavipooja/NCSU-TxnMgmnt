package simpledb.log;

import simpledb.server.SimpleDB;
import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.*;
import static simpledb.file.Page.*;
import java.util.*;


/**
 * The low-level log manager.
 * This log manager is responsible for writing log records
 * into a log file.
 * A log record can be any sequence of integer and string values.
 * The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link simpledb.tx.recovery.RecoveryMgr recovery manager}.
 * @author Edward Sciore
 */
public class LogMgr implements Iterable<BasicLogRecord> {
   /**
    * The location where the pointer to the last integer in the page is.
    * A value of 0 means that the pointer is the first value in the page.
    */
   public static final int LAST_POS = 0;
   
   /**
    * Transaction ID of Log Manager is hard coded 
    * This ID will be used when calling setInt() and setString() on the buffer objects
    * @author Team F 
    */
   public static final int LM_TXN_ID = -2;

   private String logfile;
   private Buffer mybuf;
   private Block currentblk;
   private int currentpos;

   /**
    * Creates the manager for the specified log file.
    * If the log file does not yet exist, it is created
    * with an empty first block.
    * This constructor depends on a {@link FileMgr} object
    * that it gets from the method
    * {@link simpledb.server.SimpleDB#fileMgr()}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileMgr(String)}
    * is called first.
    * @param logfile the name of the log file
    */
   public LogMgr(String logfile) {
      this.logfile = logfile;
      int logsize = SimpleDB.fileMgr().size(logfile);
      
      /**
       * If the log file size is zero, a new block will be allocated and the block will be pinned by the log manager
       * Else the last block of the log file will be used to pin to the buffer
       * @author Team F
       */
      if (logsize == 0)
         appendNewBlock();
      else {
    	  currentblk = new Block(logfile, logsize-1);
    	  this.mybuf = SimpleDB.bufferMgr().pin(currentblk);
          currentpos = getLastRecordPosition() + INT_SIZE;
      }
   }

   /**
    * Ensures that the log records corresponding to the
    * specified LSN has been written to disk.
    * All earlier log records will also be written to disk.
    * @param lsn the LSN of a log record
    */
   public void flush(int lsn) {
      if (lsn >= currentLSN())
         flush();
   }

   /**
    * Returns an iterator for the log records,
    * which will be returned in reverse order starting with the most recent.
    * @see java.lang.Iterable#iterator()
    */
   public synchronized Iterator<BasicLogRecord> iterator() {
      flush();
      return new LogIterator(currentblk);
   }

   /**
    * Appends a log record to the file.
    * The record contains an arbitrary array of strings and integers.
    * The method also writes an integer to the end of each log record whose value
    * is the offset of the corresponding integer for the previous log record.
    * These integers allow log records to be read in reverse order.
    * @param rec the list of values
    * @return the LSN of the final value
    */
   public synchronized int append(Object[] rec) {
      int recsize = INT_SIZE;  // 4 bytes for the integer that points to the previous log record
      for (Object obj : rec)
         recsize += size(obj);
      if (currentpos + recsize >= BLOCK_SIZE){ // the log record doesn't fit,
         flush();        // so move to the next block.
         appendNewBlock();
      }
      for (Object obj : rec)
         appendVal(obj);
      finalizeRecord();
      return currentLSN();
   }

   /**
    * Adds the specified value to the page at the position denoted by
    * currentpos.  Then increments currentpos by the size of the value.
    * @param val the integer or string to be added to the page
    */
   private void appendVal(Object val) {
	   int lsn = currentLSN();
	   int txn_id = LM_TXN_ID;
	   
      if (val instanceof String)
         mybuf.setString(currentpos, (String)val,txn_id,lsn);
      else
         mybuf.setInt(currentpos, (Integer)val,txn_id,lsn);
      currentpos += size(val);
   }

   /**
    * Calculates the size of the specified integer or string.
    * @param val the value
    * @return the size of the value, in bytes
    */
   private int size(Object val) {
      if (val instanceof String) {
         String sval = (String) val;
         return STR_SIZE(sval.length());
      }
      else
         return INT_SIZE;
   }

   /**
    * Returns the LSN of the most recent log record.
    * As implemented, the LSN is the block number where the record is stored.
    * Thus every log record in a block has the same LSN.
    * @return the LSN of the most recent log record
    */
   private int currentLSN() {
      return currentblk.number();
   }

   /**
    * Writes the current page to the log file.
    */
   private void flush() {
      mybuf.forceFlush();
   }

   /**
    * Clear the current page, and append it to the log file.
    */
   
   private void appendNewBlock() {
      currentpos = INT_SIZE;
      
      /**
       * Flushes the records from the log page
       * Unpins the existing buffer held by the Log Manager (if any)
       * Pins a new page in the log buffer by calling Buffer Manager
       * @author Team F
       */
      if(mybuf != null) {
    	  flush();
    	  SimpleDB.bufferMgr().unpin(mybuf);
      }
      
      mybuf = SimpleDB.bufferMgr().pinNew(logfile, new PageFormatter() {
		
		@Override
		public void format(Page p) {
			/**
			 * Write Integer 0 as the first element in the page to allow further processing
			 * This is a requirement for the log manager to work as expected as it suggests the location of last log record
			 * @author Team F
			 */
			p.setInt(0, 0);
			
		}
	});
      currentblk = mybuf.block();
      setLastRecordPosition(0);
      
//      currentblk = mybuf.append(logfile);
   }

   /**
    * Sets up a circular chain of pointers to the records in the page.
    * There is an integer added to the end of each log record
    * whose value is the offset of the previous log record.
    * The first four bytes of the page contain an integer whose value
    * is the offset of the integer for the last log record in the page.
    */
   private void finalizeRecord() {
	   /**
	    * All the transactions associated with the log manager have LM_TXN_ID as their transaction ID
	    * This value is used by buffer manager at the time of flush the log record to the disk
	    * @author Team F
	    */
      mybuf.setInt(currentpos, getLastRecordPosition(), LM_TXN_ID, currentLSN());
      setLastRecordPosition(currentpos);
      currentpos += INT_SIZE;
   }

   private int getLastRecordPosition() {
      return mybuf.getInt(LAST_POS);
   }

   private void setLastRecordPosition(int pos) {
      mybuf.setInt(LAST_POS, pos, LM_TXN_ID, currentLSN());
   }
   
   public void printLog() {
	   int bufNum = this.mybuf.getBufferId();
	   System.out.println("\n\nConents of Log :");
	   
	   Iterator<BasicLogRecord> iter = iterator();
	   while(iter.hasNext()) {
		   BasicLogRecord rec = iter.next();
		   String v1 = rec.nextString();
		   String v2 = rec.nextString();
		   
		   System.out.println("["+v1+", "+v2+"]");
	   }
	   
   }
   
   /**
    * For testing Log manager
    * Currently works when the log records in a page contain two strings
    * @author Team F
    */
   public void printLogPageBuffer() {
	   System.out.println("\n");
	   int bufNum = this.mybuf.getBufferId();
	   System.out.println("Buffer number pinned to the log block: "+bufNum);
	   System.out.println("Log block pinned to buffer : "+mybuf.block());
	   System.out.println("Conents of buffer "+bufNum+":");
	   
	   int current_rec_pos = mybuf.getInt(LogMgr.LAST_POS);
	   
	   
	   while(current_rec_pos != 0) {
		   current_rec_pos = mybuf.getInt(current_rec_pos);
//		   BasicLogRecord rec = new BasicLogRecord(pg, currentrec+INT_SIZE);
		   int position_in_rec = current_rec_pos+INT_SIZE;
		   
		   String v1 = mybuf.getString(position_in_rec);
		   position_in_rec += STR_SIZE(v1.length());
		   
		   String v2 = mybuf.getString(position_in_rec);
		   position_in_rec += STR_SIZE(v2.length());
		   
		   System.out.println("["+v1+", "+v2+"]");
	   }
	   
   }
}
