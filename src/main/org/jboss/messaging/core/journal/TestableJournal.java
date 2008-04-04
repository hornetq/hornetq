package org.jboss.messaging.core.journal;

/**
 * 
 * A TestableJournal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface TestableJournal extends Journal
{
	void checkAndReclaimFiles() throws Exception;
	
	int getDataFilesCount();
	
	int getFreeFilesCount();
	
	int getIDMapSize();
	
	//void dump();
	
}
