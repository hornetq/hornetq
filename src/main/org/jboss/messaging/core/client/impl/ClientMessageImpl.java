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

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * 
 * A ClientMessageImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 */
public class ClientMessageImpl extends MessageImpl implements ClientMessage
{
   private int deliveryCount;
   
   private ClientConsumerInternal consumer;
           
   /*
    * Constructor for when reading from network
    */
   public ClientMessageImpl(final int deliveryCount)
   {      
      super();
      
      this.deliveryCount = deliveryCount;
   }
   
   /*
    * Construct messages before sending
    */
   public ClientMessageImpl(final byte type, final boolean durable, final long expiration,
                            final long timestamp, final byte priority, MessagingBuffer body)
   {
      super(type, durable, expiration, timestamp, priority, body);
   }
   
   public ClientMessageImpl(final byte type, final boolean durable, MessagingBuffer body)
   {
      super(type, durable, 0, System.currentTimeMillis(), (byte)4, body);
   }
   
   public ClientMessageImpl(final boolean durable, MessagingBuffer body)
   {
      super((byte) 0, durable, 0, System.currentTimeMillis(), (byte)4, body);
   }
   
   /* Only used in testing */
   public ClientMessageImpl()
   {      
   }
   
   public void onReceipt(final ClientConsumerInternal consumer)
   {
      this.consumer = consumer;
   }
   
   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }
   
   public int getDeliveryCount()
   {
      return this.deliveryCount;
   }
   
   public void acknowledge() throws MessagingException
   {
      if (consumer != null)
      {
         consumer.acknowledge(this);
      }
   }
}
