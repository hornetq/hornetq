/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.util;

import static org.jboss.messaging.util.DataConstants.*;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * 
 * A ByteBufferWrapper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 */
public class ByteBufferWrapper implements MessagingBuffer
{
   private static final Charset utf8 = Charset.forName("UTF-8");
   	
	private ByteBuffer buffer;
	
	public ByteBufferWrapper(final ByteBuffer buffer)
	{
		this.buffer = buffer;
	}
	
	public ByteBuffer getBuffer()
	{
	   return buffer;
	}
	
	public byte[] array()
   {
      if(buffer.hasArray())
      {
         return buffer.array();
      }
      else
      {
         byte[] b = new byte[buffer.limit()];
         buffer.get(b);
         return b;
      }
   }
    
	public int position()
	{
		return buffer.position();
	}
	
	public void position(final int position)
   {
   	buffer.position(position);
   }

	public int capacity()
	{
		return buffer.capacity();
	}

	public void flip()
	{
		buffer.flip();
	}
	
	public MessagingBuffer slice()
   {
   	return new ByteBufferWrapper(buffer.slice());
   }
	
	public void rewind()
	{
		buffer.rewind();
	}

	public boolean getBoolean()
	{
		byte b = buffer.get();
      return (b == TRUE);
	}

	public byte getByte()
	{
		return buffer.get();
	}
	
	public short getUnsignedByte()
	{
	   return (short)(buffer.get() & 0xFF);
	}

	public void getBytes(byte[] bytes)
	{
		buffer.get(bytes);
	}
	
	public void getBytes(byte[] bytes, int offset, int length)
	{
		buffer.get(bytes, offset, length);
	}

	public double getDouble()
	{
		return buffer.getDouble();
	}

	public float getFloat()
	{
		return buffer.getFloat();
	}

	public int getInt()
	{
		return buffer.getInt();
	}
	
	public long getLong()
	{
		return buffer.getLong();
	}

	public void putNullableString(final String nullableString)
	{
		if (nullableString == null)
		{
			buffer.put(NULL);
		}
		else
		{
			buffer.put(NOT_NULL);

			putString(nullableString);
		}
	}

	public String getNullableString()
	{
		byte check = buffer.get();

		if (check == NULL)
		{
			return null;
		}
		else
		{
			return getString();
		}
	}

	public void putString(final String nullableString)
	{
		//We don't encode

		buffer.putInt(nullableString.length());

		for (int i = 0; i < nullableString.length(); i++)
		{
			buffer.putChar(nullableString.charAt(i));
		}      
	}
	
	public void putUTF(final String str) throws Exception
   {
		//TODO This is quite inefficient - can be improved using a method similar to what MINA IOBuffer does
		//(putPrefixedString)
		ByteBuffer bb = utf8.encode(str);
   	buffer.putInt(bb.limit() - bb.position());
   	buffer.put(bb);
   }

	public short getShort()
	{
		return buffer.getShort();
	}
	
	public int getUnsignedShort()
	{
	   return buffer.getShort() & 0xFFFF;
	}
	
	public char getChar()
	{
		return buffer.getChar();
	}

	public String getString()
   {
      int len = buffer.getInt();
      	
   	char[] chars = new char[len];
   	
      for (int i = 0; i < len; i++)
      {
      	chars[i] = buffer.getChar();
      }
      
      return new String(chars);               
   }
	
   public void putSimpleString(final SimpleString string)
   {
   	byte[] data = string.getData();
   	
   	buffer.putInt(data.length);
   	buffer.put(data);
   }
   
   public SimpleString getNullableSimpleString()
   {
   	int b = buffer.get();
   	if (b == NULL)
   	{
   	   return null;
   	}
   	else
   	{
         return getSimpleString();
   	}
   }
   
   public void putNullableSimpleString(final SimpleString string)
   {
   	if (string == null)
   	{
   		buffer.put(NULL);
   	}
   	else
   	{
   	   buffer.put(NOT_NULL);
   		putSimpleString(string);
   	}
   }
   
   public SimpleString getSimpleString()
   {
   	int len = buffer.getInt();
   	
   	byte[] data = new byte[len];
   	buffer.get(data);
   	
   	return new SimpleString(data);
   }
   
   public String getUTF() throws Exception
   {
   	int len = buffer.getInt();
   	byte[] data = new byte[len];
   	buffer.get(data);
   	ByteBuffer bb = ByteBuffer.wrap(data); 
   	CharBuffer cb = utf8.newDecoder().decode(bb);
   	return cb.toString();
   }

	public int limit()
	{
		return buffer.limit();
	}
	
	public void limit(final int limit)
   {
   	buffer.limit(limit);
   }

	public void putBoolean(boolean val)
	{
		if (val)
      {
         buffer.put(TRUE);
      }
		else
      {
         buffer.put(FALSE);
      }
	}

	public void putByte(byte val)
	{
		buffer.put(val);
	}

	public void putBytes(byte[] bytes)
	{
		buffer.put(bytes);
	}
	
	public void putBytes(byte[] bytes, int offset, int len)
	{
		buffer.put(bytes, offset, len);
	}

	public void putDouble(double val)
	{
		buffer.putDouble(val);
	}

	public void putFloat(float val)
	{
		buffer.putFloat(val);
	}

	public void putInt(int val)
	{
		buffer.putInt(val);
	}
	
	public void putInt(int pos, int val)
   {
      buffer.putInt(pos, val);
   }

	public void putLong(long val)
	{
		buffer.putLong(val);
	}

	public void putShort(short val)
	{
		buffer.putShort(val);
	}
	
	public void putChar(char chr)
	{
		buffer.putChar(chr);
	}
	
	public int remaining()
	{
		return buffer.remaining();
	}
}
