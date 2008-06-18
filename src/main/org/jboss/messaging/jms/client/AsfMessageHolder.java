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

package org.jboss.messaging.jms.client;

import org.jboss.messaging.core.client.ClientSession;

/**
 * 
 * A AsfMessageHolder
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class AsfMessageHolder
{
   private final JBossMessage msg;
   
   private final String consumerID;
   
   private final String queueName;
   
   private final int maxDeliveries;
   
   private final ClientSession connectionConsumerSession;
   
   public AsfMessageHolder(final JBossMessage msg, final String consumerID,
                           final String queueName, final int maxDeliveries,
                           final ClientSession connectionConsumerSession)
   {
      this.msg = msg;
      
      this.consumerID = consumerID;
      
      this.queueName = queueName;
      
      this.maxDeliveries = maxDeliveries;
      
      this.connectionConsumerSession = connectionConsumerSession;
   }

   public JBossMessage getMsg()
   {
      return msg;
   }

   public String getConsumerID()
   {
      return consumerID;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }

   public ClientSession getConnectionConsumerSession()
   {
      return connectionConsumerSession;
   }
}
