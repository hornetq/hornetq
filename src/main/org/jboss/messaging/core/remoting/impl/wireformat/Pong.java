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
import org.jboss.messaging.utils.DataConstants;

/**
 * 
 * A Pong
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Pong extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   //This is reserved for future use - for now we also pass back -1 - meaning "don't change period"
   private long newPeriod;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Pong(final long newPeriod)
   {
      super(PONG);

      this.newPeriod = newPeriod;
   }
   
   public Pong()
   {
      super(PONG);
   }

   // Public --------------------------------------------------------

   public boolean isResponse()
   {
      return true;
   }
   
   public long getNewPeriod()
   {
      return newPeriod;
   }
   
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_LONG;
   }
   

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeLong(newPeriod);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      newPeriod = buffer.readLong();
   }
   
   public boolean isWriteAlways()
   {
      return true;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", newPeriod=" + newPeriod);
      buf.append("]");
      return buf.toString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof Pong == false)
      {
         return false;
      }
            
      Pong r = (Pong)other;
      
      return super.equals(other) && this.newPeriod == r.newPeriod;
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

