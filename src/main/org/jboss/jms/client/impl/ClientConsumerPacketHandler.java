package org.jboss.jms.client.impl;

import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ClientConsumerPacketHandler implements PacketHandler
{
   private final ClientConsumer clientConsumer;

   private final String consumerID;

   /**
    * @param messageHandler
    * @param consumerID
    */
   public ClientConsumerPacketHandler(ClientConsumer clientConsumer,
         String consumerID)
   {
      this.clientConsumer = clientConsumer;
      this.consumerID = consumerID;
   }

   public String getID()
   {
      return consumerID;
   }

   public void handle(AbstractPacket packet, PacketSender sender)
   {
      try
      {
         PacketType type = packet.getType();
         if (type == PacketType.MSG_DELIVERMESSAGE)
         {
            DeliverMessage message = (DeliverMessage) packet;
            
            JBossMessage msg = JBossMessage.createMessage(message.getMessage(), message.getDeliveryID(), message.getDeliveryCount());
            
            msg.doBeforeReceive();
            
            clientConsumer.handleMessage(msg);
         }
      } catch (Exception e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

   @Override
   public String toString()
   {
      return "ConsumerAspectPacketHandler[id=" + consumerID + "]";
   }
}