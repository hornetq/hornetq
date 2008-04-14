package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_RECEIVETOKENS;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;

/**
 * 
 * A ClientProducerPacketHandler
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientProducerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ClientProducerPacketHandler.class);

   private final ClientProducerInternal clientProducer;

   private final long producerID;

   public ClientProducerPacketHandler(final ClientProducerInternal clientProducer, final long producerID)
   {
      this.clientProducer = clientProducer;
      
      this.producerID = producerID;
   }

   public long getID()
   {
      return producerID;
   }

   public void handle(final Packet packet, final PacketSender sender)
   {
      try
      {
         PacketType type = packet.getType();
         
         if (type == PROD_RECEIVETOKENS)
         {
            ProducerReceiveTokensMessage message = (ProducerReceiveTokensMessage) packet;
            
            clientProducer.receiveTokens(message.getTokens());
         }
         else
         {
         	throw new IllegalStateException("Invalid packet: " + type);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to handle packet", e);
      }
   }

   @Override
   public String toString()
   {
      return "ClientProducerPacketHandler[id=" + producerID + "]";
   }
}