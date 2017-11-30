package simpledb.test;
import simpledb.*;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;

public class LogMgrTest {
	public static void main(String args[]) {
		
		SimpleDB.initFileLogAndBufferMgr("studentdb");
		LogMgr logmgr = SimpleDB.logMgr();
		
//		int lsn1 = logmgr.append(new Object[]{"Pooja","Gosavi"});
//		logmgr.flush(lsn1);
//		int lsn2 = logmgr.append(new Object[]{"Guru Darshan","Pollepalli Manohara"});
//		logmgr.flush(lsn2);
//		int lsn3 = logmgr.append(new Object[]{"Raghav","Muddur"});
//		logmgr.flush(lsn3);
//		
//		logmgr.printLog();
//		logmgr.printLogPageBuffer();
		Buffer buff = SimpleDB.bufferMgr().pinNew("foo.txt", new PageFormatter() {
			
			@Override
			public void format(Page p) {
				p.setString(0, "Hello");
			}
		});
		BufferMgr buffmgr = SimpleDB.bufferMgr();
		Block blk = buff.block();
		
		RecoveryMgr rmgr = new  RecoveryMgr(10);
		rmgr.setString(buff, 10, "World");
		buffmgr.unpin(buff);
		rmgr.commit();
//		SimpleDB.bufferMgr().flushAll(10);
		
		
	}
}
