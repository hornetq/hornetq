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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long clientTargetID;
   
   private SimpleString address;
   
   private int windowSize;
   
   private int maxRate;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerMessage(final long clientTargetID, final SimpleString address, final int windowSize, final int maxRate)
   {
      super(SESS_CREATEPRODUCER);

      this.clientTargetID = clientTargetID;
      
      this.address = address;
      
      this.windowSize = windowSize;
      
      this.maxRate = maxRate;
   }
   
   public SessionCreateProducerMessage()
   {
      super(SESS_CREATEPRODUCER);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", windowSize=" + windowSize);
      buff.append(", maxrate=" + maxRate);
      buff.append("]");
      return buff.toString();
   }
   
   public long getClientTargetID()
   {
      return clientTargetID;
   }

   public SimpleString getAddress()
   {
      return address;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }
   
   public int getMaxRate()
   {
   	return maxRate;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(clientTargetID);
      buffer.putNullableSimpleString(address);
      buffer.putInt(windowSize);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      clientTargetID = buffer.getLong();      
      address = buffer.getNullableSimpleString();      
      windowSize = buffer.getInt();      
      maxRate = buffer.getInt();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateProducerMessage == false)
      {
         return false;
      }
            
      SessionCreateProducerMessage r = (SessionCreateProducerMessage)other;
      
      return super.equals(other) && this.clientTargetID == r.clientTargetID &&
             this.address == null ? r.address == null : this.address.equals(r.address) &&
             this.windowSize == r.windowSize &&
             this.maxRate == r.maxRate;                  
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

