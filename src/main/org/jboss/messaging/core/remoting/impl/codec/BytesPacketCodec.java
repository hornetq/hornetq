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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   protected int getBodyLength(final BytesPacket packet)
   {
   	return INT_LENGTH + packet.getBytes().length;
   }
   
   @Override
   protected void encodeBody(final BytesPacket packet, final RemotingBuffer out) throws Exception
   {
      byte[] bytes = packet.getBytes();
      
      out.putInt(bytes.length);
      
      out.put(bytes);
   }

   @Override
   protected BytesPacket decodeBody(final RemotingBuffer in) throws Exception
   {
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
