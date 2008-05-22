package org.jboss.messaging.core.journal;


/**
 * 
 * A RecordInfo
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class RecordInfo
{
	public RecordInfo(final long id, byte userRecordType, final byte[] data, final boolean isUpdate)
	{
		this.id = id;
		
		this.userRecordType = userRecordType;
		
		this.data = data;
		
		this.isUpdate = isUpdate;
	}
	
	public final long id;
	
	public final byte userRecordType;
	
	public final byte[] data;
	
	public boolean isUpdate;
	
	public byte getUserRecordType()
	{
	   return userRecordType;
	}
	
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
