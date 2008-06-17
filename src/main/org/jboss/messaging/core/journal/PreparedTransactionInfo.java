
package org.jboss.messaging.core.journal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * A PreparedTransactionInfo
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PreparedTransactionInfo
{
	public final long id;
	
	public final List<RecordInfo> records = new ArrayList<RecordInfo>();
	
	public final Set<Long> recordsToDelete = new HashSet<Long>();
	
	public PreparedTransactionInfo(final long id)
	{
		this.id = id;
	}
}
