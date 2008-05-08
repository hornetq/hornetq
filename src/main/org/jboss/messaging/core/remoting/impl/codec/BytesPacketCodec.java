/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.BYTES;

import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;
import org.jboss.messaging.util.MessagingBuffer;


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

   @Override
   protected void encodeBody(final BytesPacket packet, final MessagingBuffer out) throws Exception
   {
      byte[] bytes = packet.getBytes();
      
      out.putInt(bytes.length);
      
      out.putBytes(bytes);
   }

   @Override
   protected BytesPacket decodeBody(final MessagingBuffer in) throws Exception
   {
      int byteLength = in.getInt();
      
      byte[] bytes = new byte[byteLength];
      
      in.getBytes(bytes);

      return new BytesPacket(bytes);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
