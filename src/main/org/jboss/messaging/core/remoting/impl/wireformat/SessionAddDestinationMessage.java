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
import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A SessionAddDestinationMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddDestinationMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private SimpleString address;
   
   private boolean durable;
   
   private boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddDestinationMessage(final SimpleString address, final boolean durable, final boolean temp)
   {
      super(SESS_ADD_DESTINATION);
      
      this.address = address;
      
      this.durable = durable;
      
      this.temporary = temp;
   }
   
   public SessionAddDestinationMessage()
   {
      super(SESS_ADD_DESTINATION);
   }

   // Public --------------------------------------------------------
   
   public SimpleString getAddress()
   {
      return address;
   }
   
   public boolean isDurable()
   {
   	return durable;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(address);
      buffer.putBoolean(durable);
      buffer.putBoolean(temporary);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.getSimpleString();
      durable = buffer.getBoolean();
      temporary = buffer.getBoolean();
   }
   
   //Needs to be true so we can ensure packet has reached backup before we start sending messages to it from another
   //session
   public boolean isReplicateBlocking()
   {      
      return true;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", address=" + address + ", temp=" + durable +"]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionAddDestinationMessage == false)
      {
         return false;
      }
            
      SessionAddDestinationMessage r = (SessionAddDestinationMessage)other;
      
      return super.equals(other) && this.address.equals(r.address) &&
             this.durable == r.durable &&
             this.temporary == r.temporary;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

