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

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionScheduledSendMessage extends SessionSendMessage
{
   private long scheduledDeliveryTime;

   public SessionScheduledSendMessage(final long producerID, final ClientMessage message, final boolean requiresResponse, final long scheduledDeliveryTime)
   {
      super(SESS_SCHEDULED_SEND, producerID, message, requiresResponse);
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }

   public SessionScheduledSendMessage()
   {
      super(SESS_SCHEDULED_SEND);  
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      super.encodeBody(buffer);
      buffer.putLong(scheduledDeliveryTime);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      super.decodeBody(buffer);
      scheduledDeliveryTime = buffer.getLong();
   }

   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }
}
