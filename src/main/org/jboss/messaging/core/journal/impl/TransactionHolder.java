package org.jboss.messaging.core.journal.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.journal.RecordInfo;

/**
 * 
 * A TransactionHolder
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransactionHolder
{
	public TransactionHolder(final long id)
	{
		this.transactionID = id;
	}
	
	public final long transactionID;
	
	public final List<RecordInfo> recordInfos = new ArrayList<RecordInfo>();
	
	public final Set<Long> recordsToDelete = new HashSet<Long>();
	
	public boolean prepared;
	
}
