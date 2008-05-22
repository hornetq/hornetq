package org.jboss.messaging.core.journal;

/**
 * 
 * A TestableJournal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface TestableJournal extends Journal
{
	void checkAndReclaimFiles() throws Exception;
	
	int getDataFilesCount();
	
	int getFreeFilesCount();
	
	int getOpenedFilesCount();
	
	int getIDMapSize();
	
   String debug() throws Exception;

   void debugWait() throws Exception;
	
	//void dump();
	
}
