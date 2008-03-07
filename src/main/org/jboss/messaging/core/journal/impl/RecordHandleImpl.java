package org.jboss.messaging.core.journal.impl;

import org.jboss.messaging.core.journal.RecordHandle;

/**
 * 
 * A RecordHandleImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RecordHandleImpl implements RecordHandle
{
	private final long id;
	
	private final JournalFile file;
	
	public RecordHandleImpl(final long id, final JournalFile file)
	{
		this.id = id;
		
		this.file = file;
	}
	
	public long getID()
	{
		return id;
	}
	
	public JournalFile getFile()
	{
		return file;
	}
}
