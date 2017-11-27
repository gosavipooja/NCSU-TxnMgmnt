package simpledb.test;
import simpledb.*;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public class LogMgrTest {
	public static void main(String args[]) {
		SimpleDB.initFileLogAndBufferMgr("studentdb");
		LogMgr logmgr = SimpleDB.logMgr();
		int lsn1 = logmgr.append(new Object[]{"Pooja","Gosavi"});
		logmgr.flush(lsn1);
		int lsn2 = logmgr.append(new Object[]{"Guru Darshan","Pollepalli Manohara"});
		logmgr.flush(lsn2);
		int lsn3 = logmgr.append(new Object[]{"Raghav","Muddur"});
		logmgr.flush(lsn3);
		
		logmgr.printLog();
		logmgr.printLogPageBuffer();
	}
}
