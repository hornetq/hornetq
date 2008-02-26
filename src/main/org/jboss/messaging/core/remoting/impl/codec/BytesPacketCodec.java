/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.BYTES;

import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class BytesPacketCodec extends AbstractPacketCodec<BytesPacket>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BytesPacketCodec()
   {
      super(BYTES);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(BytesPacket packet, RemotingBuffer out)
         throws Exception
   {
      byte[] bytes = packet.getBytes();
      
      int bodyLength = INT_LENGTH + bytes.length;
      
      out.putInt(bodyLength);
      out.putInt(bytes.length);
      out.put(bytes);
   }

   @Override
   protected BytesPacket decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      int byteLength = in.getInt();
      byte[] bytes = new byte[byteLength];
      in.get(bytes);

      return new BytesPacket(bytes);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
