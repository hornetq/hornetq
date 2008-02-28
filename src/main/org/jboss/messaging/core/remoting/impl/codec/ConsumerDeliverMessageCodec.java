/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_DELIVER;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerDeliverMessage;
import org.jboss.messaging.util.StreamUtils;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ConsumerDeliverMessageCodec extends AbstractPacketCodec<ConsumerDeliverMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerDeliverMessageCodec()
   {
      super(CONS_DELIVER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ConsumerDeliverMessage message, RemotingBuffer out) throws Exception
   {
      byte[] encodedMsg = StreamUtils.toBytes(message.getMessage());
      long deliveryID = message.getDeliveryID();

      int bodyLength = encodedMsg.length
            + LONG_LENGTH + INT_LENGTH;
      out.putInt(bodyLength);

      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      out.putLong(deliveryID);
   }

   @Override
   protected ConsumerDeliverMessage decodeBody(RemotingBuffer in)
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
      Message message = new MessageImpl();
      StreamUtils.fromBytes(message, encodedMsg);
      long deliveryID = in.getLong();

      return new ConsumerDeliverMessage(message, deliveryID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
