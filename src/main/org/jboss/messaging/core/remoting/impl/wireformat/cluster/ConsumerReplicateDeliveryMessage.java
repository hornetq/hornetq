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

package org.jboss.messaging.core.remoting.impl.wireformat.cluster;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * 
 * A ConsumerReplicateDeliveryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConsumerReplicateDeliveryMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long messageID;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerReplicateDeliveryMessage(final long messageID)
   {
      super(REPLICATE_DELIVERY);

      this.messageID = messageID;
   }
   
   public ConsumerReplicateDeliveryMessage()
   {
      super(REPLICATE_DELIVERY);
   }

   // Public --------------------------------------------------------

   public long getMessageID()
   {
      return messageID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(messageID);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      messageID = buffer.getLong();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", messageID=" + messageID + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof ConsumerReplicateDeliveryMessage == false)
      {
         return false;
      }
            
      ConsumerReplicateDeliveryMessage r = (ConsumerReplicateDeliveryMessage)other;
      
      return super.equals(other) && this.messageID == r.messageID;
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
