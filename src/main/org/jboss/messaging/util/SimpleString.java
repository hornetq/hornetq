package org.jboss.messaging.util;

/**
 * 
 * A SimpleString
 * 
 * A simple String class that can store all characters byte characters, and stores as simple byte[],
 * this minimises expensive copying between String objects
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleString
{
	private final byte[] data;
	
	public SimpleString(final String string) throws Exception
	{
		int len = string.length();
		
		data = new byte[len << 1];
		
		int j = 0;
		
		for (int i = 0; i < len; i++)
		{
			char c = string.charAt(i);
			
			data[j++] = (byte)(c & 0xFF);  // low byte
			
			data[j++] = (byte)(c >> 8 & 0xFF);  // high byte
		}
	}
	
	public SimpleString(final byte[] data)
	{
		this.data = data;
	}
	
	public byte[] getData()
	{
		return data;
	}
	
	public String asString() throws Exception
	{
		int len = data.length >> 1;
		
		char[] chars = new char[len];
		
		int j = 0;
		
		for (int i = 0; i < len; i++)
		{
			chars[i] = (char)(data[j++] | data[j++] << 8);
		}
		
		return new String(chars);
	}
}