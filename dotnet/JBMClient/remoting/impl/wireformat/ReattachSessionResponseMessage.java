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
 * 
 * A ReattachSessionResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReattachSessionResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int lastReceivedCommandID;
   
   //Is this flag really necessary - try removing it
   private boolean removed;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReattachSessionResponseMessage(final int lastReceivedCommandID, final boolean removed)
   {
      super(REATTACH_SESSION_RESP);

      this.lastReceivedCommandID = lastReceivedCommandID;
      
      this.removed = removed;
   }
   
   public ReattachSessionResponseMessage()
   {
      super(REATTACH_SESSION_RESP);
   }

   // Public --------------------------------------------------------

   public int getLastReceivedCommandID()
   {
      return lastReceivedCommandID;
   }
   
   public boolean isRemoved()
   {
      return removed;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   { 
      buffer.putInt(lastReceivedCommandID);
      buffer.putBoolean(removed);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   { 
      lastReceivedCommandID = buffer.getInt();
      removed = buffer.getBoolean();
   }
   
   public boolean isResponse()
   {      
      return true;
   }

   public boolean equals(Object other)
   {
      if (other instanceof ReattachSessionResponseMessage == false)
      {
         return false;
      }
            
      ReattachSessionResponseMessage r = (ReattachSessionResponseMessage)other;
      
      return super.equals(other) && this.lastReceivedCommandID == r.lastReceivedCommandID;
   }
   
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

