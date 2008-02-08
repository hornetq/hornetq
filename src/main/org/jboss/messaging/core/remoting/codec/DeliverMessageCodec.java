/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELIVER;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class DeliverMessageCodec extends AbstractPacketCodec<DeliverMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeliverMessageCodec()
   {
      super(SESS_DELIVER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(DeliverMessage message, RemotingBuffer out) throws Exception
   {
      byte[] encodedMsg = encodeMessage(message.getMessage());
      long deliveryID = message.getDeliveryID();

      int bodyLength = encodedMsg.length
            + LONG_LENGTH + INT_LENGTH;
      out.putInt(bodyLength);

      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      out.putLong(deliveryID);
   }

   @Override
   protected DeliverMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message msg = decodeMessage(encodedMsg);
      long deliveryID = in.getLong();

      return new DeliverMessage(msg, deliveryID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
