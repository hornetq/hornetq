/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerDeliverMessage;
import org.jboss.messaging.core.server.Delivery;

/**
 * 
 * A DeliveryImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class DeliveryImpl implements Delivery
{
   private static final Logger log = Logger.getLogger(DeliveryImpl.class);
   
   private final MessageReference reference;
   
   private final long sessionID;

   private final long consumerID;
   
   private final long deliveryID;
   
   private final PacketSender sender;

   public DeliveryImpl(final MessageReference reference, 
                       final long sessionID, final long consumerID,
                       final long deliveryID, final PacketSender sender)
   {      
      this.reference = reference;
      this.sessionID = sessionID;
      this.consumerID = consumerID;
      this.deliveryID = deliveryID;
      this.sender = sender;
   }

   public MessageReference getReference()
   {
      return reference;
   }

   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public void deliver() throws Exception
   {
      /*
      Note we copy the message before sending.
      This is because delivery count may be different for the same message sent to different topic subscribers
      And invm the same message instance would otherwise be passed to all consumers
      For the non INVM case this copy is unncessary and can be optimised away TODO - although the overhead of
      copying is actually quite small
      */
      Message copy = reference.getMessage().copy();
      
      copy.setDeliveryCount(reference.getDeliveryCount() + 1);
      
      ConsumerDeliverMessage message = new ConsumerDeliverMessage(copy, deliveryID);
      
      message.setTargetID(consumerID);
      message.setExecutorID(sessionID);
      
      sender.send(message);
   }
}
