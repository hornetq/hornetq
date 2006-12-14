/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server.endpoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * A DeliveryRecovery
 * 
 * Used for sending information about to recover a delivery to the server
 * on failover
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DeliveryRecovery implements Streamable
{   
   private long deliveryId;
   
   private long messageId;
   
   private long channelId;
   
   public DeliveryRecovery()
   {      
   }
   
   public DeliveryRecovery(long deliveryId, long messageId, long channelId)
   {
      this.deliveryId = deliveryId;
      
      this.messageId = messageId;
      
      this.channelId = channelId;
   }
   
   public long getDeliveryId()
   {
      return deliveryId;
   }
   
   public long getMessageId()
   {
      return messageId;
   }
   
   public long getChannelId()
   {
      return channelId;
   }

   public void read(DataInputStream in) throws Exception
   {
      deliveryId = in.readLong();
      
      messageId = in.readLong();
      
      channelId = in.readLong();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeLong(deliveryId);
      
      out.writeLong(messageId);
      
      out.writeLong(channelId);
   }
}
