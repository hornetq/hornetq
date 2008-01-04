/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoder;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;
import org.apache.mina.filter.codec.demux.MessageEncoder;
import org.jboss.messaging.core.remoting.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.codec.DecoderStatus;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaPacketCodec<P extends AbstractPacket> implements
      MessageEncoder<P>, MessageDecoder
{
   // Constants -----------------------------------------------------

   private static final byte TRUE = (byte) 0;

   private static final byte FALSE = (byte) 1;

   // used to terminate encoded Strings
   public static final byte NULL_BYTE = (byte) 0;

   public static final byte NULL_STRING = (byte) 0;

   public static final byte NOT_NULL_STRING = (byte) 1;

   public static final CharsetEncoder UTF_8_ENCODER = Charset.forName("UTF-8")
         .newEncoder();

   private static final CharsetDecoder UTF_8_DECODER = Charset.forName("UTF-8")
         .newDecoder();

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   private final AbstractPacketCodec<P> codec;

   public MinaPacketCodec(AbstractPacketCodec<P> codec)
   {
      this.codec = codec;
   }

   // Public --------------------------------------------------------

   // MessageEncoder implementation ---------------------------------

   public void encode(IoSession session, P packet, ProtocolEncoderOutput out)
         throws Exception
   {
      final IoBuffer buffer = IoBuffer.allocate(256);
      // Enable auto-expand for easier encoding
      buffer.setAutoExpand(true);

      RemotingBuffer wrapper = new BufferWrapper(buffer);

      codec.encode(packet, wrapper);

      buffer.flip();
      out.write(buffer);
   }

   // MessageDecoder implementation ---------------------------------

   public MessageDecoderResult decodable(IoSession session, IoBuffer in)
   {
      RemotingBuffer wrapper = new BufferWrapper(in);

      DecoderStatus status = codec.decodable(wrapper);
      return convertToMina(status);
   }

   public MessageDecoderResult decode(IoSession session, IoBuffer in,
         ProtocolDecoderOutput out) throws Exception
   {
      RemotingBuffer wrapper = new BufferWrapper(in);

      P packet = codec.decode(wrapper);

      if (packet == null)
      {
         return MessageDecoder.NEED_DATA;
      }

      out.write(packet);

      return MessageDecoder.OK;
   }

   private MessageDecoderResult convertToMina(DecoderStatus status)
   {
      if (status == DecoderStatus.OK)
      {
         return MessageDecoderResult.OK;
      }
      if (status == DecoderStatus.NEED_DATA)
      {
         return MessageDecoderResult.NEED_DATA;
      }
      return MessageDecoderResult.NOT_OK;
   }

   public void finishDecode(IoSession session, ProtocolDecoderOutput out)
         throws Exception
   {
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static class BufferWrapper implements RemotingBuffer
   {
      protected final IoBuffer buffer;

      public BufferWrapper(IoBuffer buffer)
      {
         assert buffer != null;

         this.buffer = buffer;
      }

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
   }
}
