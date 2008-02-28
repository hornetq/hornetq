package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerDeliverMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ClientConsumerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ClientConsumerPacketHandler.class);

   private final ClientConsumerInternal clientConsumer;

   private final String consumerID;

   public ClientConsumerPacketHandler(final ClientConsumerInternal clientConsumer, final String consumerID)
   {
      this.clientConsumer = clientConsumer;
      
      this.consumerID = consumerID;
   }

   public String getID()
   {
      return consumerID;
   }

   public void handle(final Packet packet, final PacketSender sender)
   {
      try
      {
         PacketType type = packet.getType();
         
         if (type == PacketType.CONS_DELIVER)
         {
            ConsumerDeliverMessage message = (ConsumerDeliverMessage) packet;
            
            clientConsumer.handleMessage(message);
         }
         else
         {
         	throw new IllegalStateException("Invalid packet: " + type);
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
      return "ClientConsumerPacketHandler[id=" + consumerID + "]";
   }
}