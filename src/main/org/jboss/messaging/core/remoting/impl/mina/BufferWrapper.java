/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.FALSE;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.TRUE;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;

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

   public int remaining()
   {
      return buffer.remaining();
   }

   public void put(byte byteValue)
   {
      buffer.put(byteValue);
   }

   public void put(byte[] byteArray)
   {
      buffer.put(byteArray);
   }

   public void putInt(int intValue)
   {
      buffer.putInt(intValue);
   }

   public void putLong(long longValue)
   {
      buffer.putLong(longValue);
   }

   public void putFloat(float floatValue)
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

   public void putBoolean(boolean b)
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

   public void putNullableString(String nullableString)
         throws CharacterCodingException
   {

      if (nullableString == null)
      {
         buffer.put(NULL_STRING);
      } else
      {
         buffer.put(NOT_NULL_STRING);
         buffer.putString(nullableString, UTF_8_ENCODER);
         buffer.put(NULL_BYTE);
      }
   }

   public String getNullableString() throws CharacterCodingException
   {
      byte check = buffer.get();
      if (check == NULL_STRING)
      {
         return null;
      } else
      {
         assert check == NOT_NULL_STRING;

         return buffer.getString(UTF_8_DECODER);
      }
   }
   
   public void rewind()
   {
   	buffer.rewind();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}