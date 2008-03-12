package org.jboss.messaging.core.journal.impl;

import java.util.ArrayList;
import java.util.List;

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
	
	private List<JournalFile> files = new ArrayList<JournalFile>();
	
	public RecordHandleImpl(final long id)
	{
		this.id = id;
	}
	
	public void addFile(JournalFile file)
	{
		files.add(file);
		
		file.incRefCount();
	}
	
	public long getID()
	{
		return id;
	}
	
	public void recordDeleted()
	{
		for (JournalFile file: files)
		{
			file.decRefCount();
		}
	}
}
