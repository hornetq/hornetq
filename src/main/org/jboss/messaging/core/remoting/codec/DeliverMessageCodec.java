/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELIVERMESSAGE;

import org.jboss.messaging.core.contract.Message;
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
      super(MSG_DELIVERMESSAGE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(DeliverMessage message, RemotingBuffer out) throws Exception
   {
      Message msg = message.getMessage();
      byte[] encodedMsg = encode(message.getMessage());
      String consumerID = message.getConsumerID();
      long deliveryID = message.getDeliveryID();
      int deliveryCount = message.getDeliveryCount();

      int bodyLength = 1 + INT_LENGTH + encodedMsg.length + sizeof(consumerID)
            + LONG_LENGTH + INT_LENGTH;
      out.putInt(bodyLength);

      out.put(msg.getType());
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      out.putNullableString(consumerID);
      out.putLong(deliveryID);
      out.putInt(deliveryCount);
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

      byte type = in.get();
      int msgLength = in.getInt();
      byte[] encodedMsg = new byte[msgLength];
      in.get(encodedMsg);
      Message msg = decode(type, encodedMsg);
      String consumerID = in.getNullableString();
      long deliveryID = in.getLong();
      int deliveryCount = in.getInt();

      return new DeliverMessage(msg, consumerID, deliveryID, deliveryCount);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
