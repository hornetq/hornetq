package org.jboss.messaging.core.journal.impl;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.journal.RecordHandle;
import org.jboss.messaging.core.journal.RecordHistory;

/**
 * 
 * A RecordHistoryImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RecordHistoryImpl implements RecordHistory
{
	private final List<byte[]> records = new ArrayList<byte[]>();
	
	private final RecordHandle handle;
	
	public RecordHistoryImpl(final RecordHandle handle)
	{
		this.handle = handle;
	}
	
	public RecordHandle getHandle()
	{
		return handle;
	}

	public List<byte[]> getRecords()
	{
		return records;
	}
	
	public void addRecord(final byte[] record)
	{
		records.add(record);
	}	
}
