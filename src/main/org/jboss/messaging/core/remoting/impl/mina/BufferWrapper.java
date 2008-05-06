/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.util.DataConstants.FALSE;
import static org.jboss.messaging.util.DataConstants.NOT_NULL;
import static org.jboss.messaging.util.DataConstants.NULL;
import static org.jboss.messaging.util.DataConstants.TRUE;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A BufferWrapper
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BufferWrapper implements RemotingBuffer
{
   // Constants -----------------------------------------------------
	
   // Attributes ----------------------------------------------------

   protected final IoBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BufferWrapper(IoBuffer buffer)
   {
      assert buffer != null;

      this.buffer = buffer;
   }
   
   // Public --------------------------------------------------------

   // RemotingBuffer implementation ----------------------------------------------

   public byte[] array()
   {
      return buffer.array();
   }

   public int remaining()
   {
      return buffer.remaining();
   }

   public void put(final byte byteValue)
   {
      buffer.put(byteValue);
   }

   public void put(final byte[] byteArray)
   {
      buffer.put(byteArray);
   }

   public void putInt(final int intValue)
   {
      buffer.putInt(intValue);
   }

   public void putLong(final long longValue)
   {
      buffer.putLong(longValue);
   }

   public void putFloat(final float floatValue)
   {
      buffer.putFloat(floatValue);
   }

   public byte get()
   {
      return buffer.get();
   }

   public void get(byte[] b)
   {
      buffer.get(b);
   }

   public int getInt()
   {
      return buffer.getInt();
   }

   public long getLong()
   {
      return buffer.getLong();
   }

   public float getFloat()
   {
      return buffer.getFloat();
   }

   public void putBoolean(final boolean b)
   {
      if (b)
      {
         buffer.put(TRUE);
      }
      else
      {
         buffer.put(FALSE);
      }
   }

   public boolean getBoolean()
   {
      byte b = buffer.get();
      return (b == TRUE);
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
   
   public void putSimpleString(final SimpleString string)
   {
   	byte[] data = string.getData();
   	
   	buffer.putInt(data.length);
   	buffer.put(data);
   }
   
   public SimpleString getSimpleString()
   {
   	int len = buffer.getInt();
   	
   	byte[] data = new byte[len];
   	buffer.get(data);
   	
   	return new SimpleString(data);
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
         
   public void rewind()
   {
   	buffer.rewind();
   }

   public void flip()
   {
      buffer.flip();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void putString(final String nullableString)
   {
      buffer.putInt(nullableString.length());
      
      for (int i = 0; i < nullableString.length(); i++)
      {
         buffer.putChar(nullableString.charAt(i));
      }      
   }
   
   private String getString()
   {
      int len = buffer.getInt();
         
      char[] chars = new char[len];
      
      for (int i = 0; i < len; i++)
      {
         chars[i] = buffer.getChar();
      }
      
      return new String(chars);               
   }
   
   // Inner classes -------------------------------------------------
}