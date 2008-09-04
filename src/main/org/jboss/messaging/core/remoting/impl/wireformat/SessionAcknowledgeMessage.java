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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionAcknowledgeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private long deliveryID;
   
   private boolean allUpTo;
   
   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAcknowledgeMessage(final long deliveryID, final boolean allUpTo,
                                    final boolean requiresResponse)
   {
      super(SESS_ACKNOWLEDGE);
      
      this.deliveryID = deliveryID;
      
      this.allUpTo = allUpTo;
      
      this.requiresResponse = requiresResponse;
   }
   
   public SessionAcknowledgeMessage()
   {
      super(SESS_ACKNOWLEDGE);
   }

   // Public --------------------------------------------------------
   
   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }
      
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public boolean isAllUpTo()
   {
      return allUpTo;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {      
      buffer.putLong(deliveryID);
      buffer.putBoolean(allUpTo);
      buffer.putBoolean(requiresResponse);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      deliveryID = buffer.getLong();
      allUpTo = buffer.getBoolean();
      requiresResponse = buffer.getBoolean();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionAcknowledgeMessage == false)
      {
         return false;
      }
            
      SessionAcknowledgeMessage r = (SessionAcknowledgeMessage)other;
      
      return super.equals(other) && this.deliveryID == r.deliveryID &&
             this.allUpTo == r.allUpTo && this.requiresResponse == r.requiresResponse;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

