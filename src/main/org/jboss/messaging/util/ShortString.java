package org.jboss.messaging.util;

/**
 * 
 * A ShortString
 * 
 * A simple String class that only stores single byte characters, and stores as simple byte[],
 * this minimises expensive copying between String objects
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ShortString
{
	private final byte[] data;
	
	public ShortString(final String string)
	{
		int len = string.length();
		
		data = new byte[len];
		
		for (int i = 0; i < len; i++)
		{
			char c = string.charAt(i);
			
			if (c > 0xFF)
			{
				throw new IllegalArgumentException("Cannot encode string - contains multi-byte character(s)");
			}
			
			data[i] = (byte)c;  // low byte
		}
	}
	
	public ShortString(final byte[] data)
	{
		this.data = data;
	}
	
	public byte[] getData()
	{
		return data;
	}
	
	public String asString() throws Exception
	{
		int len = data.length;
		
		char[] chars = new char[len];
		
		for (int i = 0; i < len; i++)
		{
			chars[i] = (char)data[i];
		}
		
		return new String(chars);
	}
}
