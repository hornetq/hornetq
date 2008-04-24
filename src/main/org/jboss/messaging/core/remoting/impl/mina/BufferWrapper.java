/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.FALSE;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.TRUE;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;

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
	
   // used to terminate encoded Strings
   public static final byte NULL_BYTE = (byte) 0;

   public static final byte NULL_STRING = (byte) 0;

   public static final byte NOT_NULL_STRING = (byte) 1;

   public static final CharsetEncoder UTF_8_ENCODER = Charset.forName("UTF-8")
         .newEncoder();

   public static final CharsetDecoder UTF_8_DECODER = Charset.forName("UTF-8")
         .newDecoder();

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
      } else
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
         buffer.put(NULL_STRING);
      }
      else
      {
         buffer.put(NOT_NULL_STRING);
         putString(nullableString);
      }
   }

   public String getNullableString()
   {
      byte check = buffer.get();
      if (check == NULL_STRING)
      {
         return null;
      }
      else
      {
         return getString();
      }
   }
   
   public void putString(final String string)
   {
   	int len = string.length();
      buffer.putInt(len);
      for (int i = 0; i < len; i++)
      {   
      	buffer.putChar(string.charAt(i));
      }
   }

   public String getString()
   {
   	int len = buffer.getInt();
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
      	chars[i] = buffer.getChar();
      }
                     
      String string =  new String(chars);
      
      return string;
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

   // Inner classes -------------------------------------------------
}