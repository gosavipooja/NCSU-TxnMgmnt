package simpledb.log;
import simpledb.*;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.RecoveryMgr;

public class LogMgrUnitTest {
	public static void main(String args[]) {
		
		SimpleDB.initFileLogAndBufferMgr("studentdb");
//		SimpleDB.init("studentDB");
		LogMgr logmgr = SimpleDB.logMgr();
		
		int lsn1 = logmgr.append(new Object[]{"Foo","Bar"});
		logmgr.flush(lsn1);
		int lsn2 = logmgr.append(new Object[]{"ABC","XYZ"});
		logmgr.flush(lsn2);
		int lsn3 = logmgr.append(new Object[]{"Hello","World"});
		logmgr.flush(lsn3);
		
//		logmgr.printLog();
		logmgr.printLogPageBuffer();
	}
}
