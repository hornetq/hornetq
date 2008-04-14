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
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.demux.MessageEncoder;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaEncoder implements MessageEncoder<Packet>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Map<PacketType, AbstractPacketCodec> codecs;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

    MinaEncoder()
   {
      codecs = new HashMap<PacketType, AbstractPacketCodec>();
   }
   // Public --------------------------------------------------------

   public void put(PacketType type, AbstractPacketCodec codec)
   {
      codecs.put(type, codec);
   }
   
   // MessageEncoder implementation --------------------------------
   
   public void encode(IoSession session, Packet packet,
         ProtocolEncoderOutput out) throws Exception
   {
      final IoBuffer buffer = IoBuffer.allocate(1024);
      // Enable auto-expand for easier encoding
      buffer.setAutoExpand(true);

      RemotingBuffer wrapper = new BufferWrapper(buffer);

      AbstractPacketCodec codec = codecs.get(packet.getType());
      if (codec == null)
      {
         throw new IllegalStateException("no encoder has been registered for " + packet);
      }
      codec.encode(packet, wrapper);

      buffer.flip();
      out.write(buffer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
