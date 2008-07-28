/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerFlowCreditMessage;

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

   public void handle(final long sessionID, final Packet packet)
   {    
      byte type = packet.getType();
      
      if (type == PacketImpl.PROD_RECEIVETOKENS)
      {
         ProducerFlowCreditMessage message = (ProducerFlowCreditMessage) packet;
         
         try
         {
            clientProducer.receiveCredits(message.getTokens());
         }
         catch (Exception e)
         {
            log.error("Failed to handle packet " + packet, e);
         }
      }
      else
      {
      	throw new IllegalStateException("Invalid packet: " + type);
      }      
   }

   @Override
   public String toString()
   {
      return "ClientProducerPacketHandler[id=" + producerID + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof ClientProducerPacketHandler == false)
      {
         return false;
      }
            
      ClientProducerPacketHandler r = (ClientProducerPacketHandler)other;
      
      return r.producerID == this.producerID;     
   }
}