/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import java.io.Serializable;


/**
 * 
 * A SimpleString
 * 
 * A simple String class that can store all characters, and stores as simple byte[],
 * this minimises expensive copying between String objects
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleString implements CharSequence, Serializable
{
   private static final long serialVersionUID = 4204223851422244307L;

   // Attributes
	// ------------------------------------------------------------------------
	private final byte[] data;
	
	private transient int hash;
	
	//Cache the string
	private transient String str;
	
	// Constructors
	// ----------------------------------------------------------------------
		
	public SimpleString(final String string)
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
		
		str = string;
	}
	
	public SimpleString(final byte[] data)
	{
		this.data = data;
	}
	
	// CharSequence implementation
	// ---------------------------------------------------------------------------
	
	public int length()
	{
		return data.length >> 1;
	}
	
	public char charAt(int pos)
	{
		if (pos < 0 || pos >= data.length >> 1)
		{
			throw new IndexOutOfBoundsException();
		}
		pos <<= 1;
		
		return (char)(data[pos] | data[pos + 1] << 8);
	}
	
	public CharSequence subSequence(final int start, final int end)
	{
		int len = data.length >> 1;

		if (end < start || start < 0 || end > len)
		{
			throw new IndexOutOfBoundsException();
		}
		else
		{
			int newlen = (end - start) << 1;
			byte[] bytes = new byte[newlen];
			
			System.arraycopy(data, start << 1, bytes, 0, newlen);
			
			return new SimpleString(bytes);
		}
	}
	
	// Public
	// ---------------------------------------------------------------------------
	
	public byte[] getData()
	{
		return data;
	}
		
	public boolean startsWith(final SimpleString other)
	{
		byte[] otherdata = other.data;
		
		if (otherdata.length > this.data.length)
		{
			return false;
		}
		
		for (int i = 0; i < otherdata.length; i++)
		{
			if (this.data[i] != otherdata[i])
			{
				return false;
			}
		}
		
		return true;
	}
		
	public String toString()
	{
		if (str == null)
		{
   		int len = data.length >> 1;
   		
   		char[] chars = new char[len];
   		
   		int j = 0;
   		
   		for (int i = 0; i < len; i++)
   		{
   			chars[i] = (char)(data[j++] | data[j++] << 8);
   		}
   		
   		str =  new String(chars);
		}
		
		return str;
	}
	
	public boolean equals(Object other)
	{		
		if (other instanceof SimpleString)
		{
   		SimpleString s = (SimpleString)other;
   		
   		if (data.length != s.data.length)
   		{
   			return false;
   		}
   		
   		for (int i = 0; i < data.length; i++)
   		{
   			if (data[i] != s.data[i])
   			{
   				return false;
   			}
   		}
   		
   		return true;
		}
		else
		{
			return false;
		}
	}
	
	public int hashCode()
	{
		if (hash == 0)
		{
			for (int i = 0; i < data.length; i++)
			{
            hash = 31 * hash + data[i];
        }
		}
		
		return hash;
	}
	
	public static int sizeofString(final SimpleString str)
	{
		return SIZE_INT + str.data.length;
	}
}