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
public class SessionCreateProducerResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int initialCredits;
   
   private int maxRate;

   private SimpleString autoGroupId;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessage(final int initialCredits, final int maxRate, final SimpleString autoGroupId)
   {
      super(SESS_CREATEPRODUCER_RESP);
 
      this.initialCredits = initialCredits;
      
      this.maxRate = maxRate;

      this.autoGroupId = autoGroupId;
   }
   
   public SessionCreateProducerResponseMessage()
   {
      super(SESS_CREATEPRODUCER_RESP);
   }

   // Public --------------------------------------------------------

   public boolean isResponse()
   {
      return true;
   }
   
   public int getInitialCredits()
   {
   	return initialCredits;
   }
   
   public int getMaxRate()
   {
   	return maxRate;
   }


   public SimpleString getAutoGroupId()
   {
      return autoGroupId;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(initialCredits);
      buffer.putInt(maxRate);
      buffer.putNullableSimpleString(autoGroupId);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {     
      initialCredits = buffer.getInt();
      maxRate = buffer.getInt();
      autoGroupId = buffer.getNullableSimpleString();
   }
   

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", initialCredits=" + initialCredits);
      buf.append(", maxRate=" + maxRate);
      buf.append("]");
      return buf.toString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateProducerResponseMessage == false)
      {
         return false;
      }
            
      SessionCreateProducerResponseMessage r = (SessionCreateProducerResponseMessage)other;
      
      return super.equals(other) &&
         this.initialCredits == r.initialCredits &&
         this.maxRate == r.maxRate;
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
