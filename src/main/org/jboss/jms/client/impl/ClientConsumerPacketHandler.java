package org.jboss.jms.client.impl;

import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ClientConsumerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);

   private final ClientConsumerInternal clientConsumer;

   private final String consumerID;

   public ClientConsumerPacketHandler(ClientConsumerInternal clientConsumer, String consumerID)
   {
      this.clientConsumer = clientConsumer;
      
      this.consumerID = consumerID;
   }

   public String getID()
   {
      return consumerID;
   }

   public void handle(Packet packet, PacketSender sender)
   {
      try
      {
         PacketType type = packet.getType();
         if (type == PacketType.SESS_DELIVER)
         {
            DeliverMessage message = (DeliverMessage) packet;
            
            clientConsumer.handleMessage(message);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to handle message", e);
      }
   }

   @Override
   public String toString()
   {
      return "ConsumerAspectPacketHandler[id=" + consumerID + "]";
   }
}