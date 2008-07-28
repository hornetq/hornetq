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

import org.jboss.messaging.core.remoting.MessagingBuffer;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCancelMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private long deliveryID;
   
   private boolean expired;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCancelMessage(final long deliveryID, final boolean expired)
   {
      super(SESS_CANCEL);
      
      this.deliveryID = deliveryID;
      
      this.expired = expired;
   }
   
   public SessionCancelMessage()
   {
      super(SESS_CANCEL);
   }

   // Public --------------------------------------------------------
   
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public boolean isExpired()
   {
      return expired;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(deliveryID);
      buffer.putBoolean(expired);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      deliveryID = buffer.getLong();
      expired = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", deliveryID=" + deliveryID + ", expired=" + expired + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCancelMessage == false)
      {
         return false;
      }
            
      SessionCancelMessage r = (SessionCancelMessage)other;
      
      return super.equals(other) && this.deliveryID == r.deliveryID &&
             this.expired == r.expired;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

