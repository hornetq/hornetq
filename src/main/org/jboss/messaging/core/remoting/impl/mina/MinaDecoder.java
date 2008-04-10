/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoder;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.DecoderStatus;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaDecoder implements MessageDecoder
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Map<PacketType, AbstractPacketCodec> codecs;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaDecoder()
   {
      codecs = new HashMap<PacketType, AbstractPacketCodec>();
   }

   // Public --------------------------------------------------------

   public void put(PacketType type, AbstractPacketCodec codec)
   {
      codecs.put(type, codec);
   }
   
   // MessageDecoder implementation ---------------------------------

   public MessageDecoderResult decodable(IoSession session, IoBuffer in)
   {
      byte byteValue = in.get();
      PacketType type = PacketType.from(byteValue);
      if (!codecs.containsKey(type))
         return MessageDecoderResult.NOT_OK;

      AbstractPacketCodec codec = codecs.get(type);
      RemotingBuffer wrapper = new BufferWrapper(in);

      DecoderStatus status = codec.decodable(wrapper);
      return convertToMina(status);
   }

   public MessageDecoderResult decode(IoSession session, IoBuffer in,
         ProtocolDecoderOutput out) throws Exception
   {
      byte byteValue = in.get();
      PacketType type = PacketType.from(byteValue);
      AbstractPacketCodec codec = codecs.get(type);
      // rewind from 1
      in.position(in.position() -1);
      Packet packet = codec.decode(new BufferWrapper(in));
      if (packet == null)
         return MessageDecoderResult.NEED_DATA;
      
      out.write(packet);
      return MessageDecoderResult.OK;
   }

   public void finishDecode(IoSession session, ProtocolDecoderOutput out)
         throws Exception
   {
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

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
   // Inner classes -------------------------------------------------
}
