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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   //TODO - remove this when in next stage of refactoring
   private byte[] encodedMsg;
   
   protected int getBodyLength(final ConsumerDeliverMessage packet) throws Exception
   {
   	encodedMsg = StreamUtils.toBytes(packet.getMessage());
   	
   	return INT_LENGTH + encodedMsg.length + LONG_LENGTH; 
   }
   
   @Override
   protected void encodeBody(final ConsumerDeliverMessage message, final RemotingBuffer out) throws Exception
   {
      out.putInt(encodedMsg.length);
      out.put(encodedMsg);
      out.putLong(message.getDeliveryID());
      encodedMsg = null;
   }

   @Override
   protected ConsumerDeliverMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
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
