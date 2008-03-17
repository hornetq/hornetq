package org.jboss.messaging.core.journal;


/**
 * 
 * A RecordInfo
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RecordInfo
{
	public RecordInfo(final long id, final byte[] data, final boolean isUpdate)
	{
		this.id = id;
		
		this.data = data;
		
		this.isUpdate = isUpdate;
	}
	
	public final long id;
	
	public final byte[] data;
	
	public boolean isUpdate;
	
	public int hashCode()
	{
		return (int)((id >>> 32) ^ id);
	}
	
	public boolean equals(Object other)
	{
		RecordInfo r = (RecordInfo)other;
		
		return r.id == this.id;		
	}
	
}
