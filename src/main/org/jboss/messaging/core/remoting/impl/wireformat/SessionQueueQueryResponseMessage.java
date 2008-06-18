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

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionQueueQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessage extends PacketImpl
{
   private boolean exists;
   
   private boolean durable;
   
   private boolean temporary;
   
   private int maxSize;
   
   private int consumerCount;
   
   private int messageCount;
   
   private SimpleString filterString;
   
   private SimpleString address;
   
   public SessionQueueQueryResponseMessage(final boolean durable, final boolean temporary, final int maxSize, 
   		final int consumerCount, final int messageCount, final SimpleString filterString, final SimpleString address)
   {
   	this(durable, temporary, maxSize, consumerCount, messageCount, filterString, address, true);
   }
   
   public SessionQueueQueryResponseMessage()
   {
      this(false, false, 0, 0, 0, null, null, false);
   }
   
   private SessionQueueQueryResponseMessage(final boolean durable, final boolean temporary, final int maxSize, 
   		final int consumerCount, final int messageCount, final SimpleString filterString, final SimpleString address,
   		final boolean exists)
   {
      super(SESS_QUEUEQUERY_RESP);
       
      this.durable = durable;
      
      this.temporary = temporary;
      
      this.maxSize = maxSize;
      
      this.consumerCount = consumerCount;
      
      this.messageCount = messageCount;
      
      this.filterString = filterString;
      
      this.address = address;
      
      this.exists = exists;      
   }
      
   public boolean isExists()
   {
      return exists;
   }
   
   public boolean isDurable()
   {
      return durable;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }
   
   public int getMaxSize()
   {
      return maxSize;
   }
   
   public int getConsumerCount()
   {
      return consumerCount;
   }
   
   public int getMessageCount()
   {
      return messageCount;
   }
   
   public SimpleString getFilterString()
   {
      return filterString;
   }
   
   public SimpleString getAddress()
   {
      return address;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(exists);
      buffer.putBoolean(durable);
      buffer.putBoolean(temporary);
      buffer.putInt(maxSize);
      buffer.putInt(consumerCount);
      buffer.putInt(messageCount);
      buffer.putNullableSimpleString(filterString);
      buffer.putNullableSimpleString(address);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      exists = buffer.getBoolean();
      durable = buffer.getBoolean();
      temporary = buffer.getBoolean();
      maxSize = buffer.getInt();
      consumerCount = buffer.getInt();
      messageCount = buffer.getInt();
      filterString  = buffer.getNullableSimpleString();
      address = buffer.getNullableSimpleString();
   }
   
}
