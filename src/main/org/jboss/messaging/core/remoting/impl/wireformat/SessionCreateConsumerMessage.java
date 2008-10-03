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
public class SessionCreateConsumerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;
   
   private SimpleString filterString;
   
   private int windowSize;
   
   private int maxRate;
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerMessage(final SimpleString queueName, final SimpleString filterString,   		                              
   		                              final int windowSize, final int maxRate)
   {
      super(SESS_CREATECONSUMER);

      this.queueName = queueName;
      this.filterString = filterString;
      this.windowSize = windowSize;
      this.maxRate = maxRate;
   }
   
   public SessionCreateConsumerMessage()
   {
      super(SESS_CREATECONSUMER);   
   }   

   // Public --------------------------------------------------------

   public boolean isReHandleResponseOnFailure()
   {
      return true;
   }
   
   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", windowSize=" + windowSize);
      buff.append(", maxRate=" + maxRate);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
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
      buffer.putSimpleString(queueName);
      buffer.putNullableSimpleString(filterString);
      buffer.putInt(windowSize);
      buffer.putInt(maxRate);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.getSimpleString();
      filterString = buffer.getNullableSimpleString();
      windowSize = buffer.getInt();
      maxRate = buffer.getInt();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateConsumerMessage == false)
      {
         return false;
      }
            
      SessionCreateConsumerMessage r = (SessionCreateConsumerMessage)other;
      
      return super.equals(other) && 
             this.queueName.equals(r.queueName) &&
             this.filterString == null ? r.filterString == null : this.filterString.equals(r.filterString) &&
             this.windowSize == r.windowSize &&
             this.maxRate == r.maxRate;                  
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
