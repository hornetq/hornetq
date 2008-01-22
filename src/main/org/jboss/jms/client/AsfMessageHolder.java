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
package org.jboss.jms.client;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.message.JBossMessage;

/**
 * 
 * A AsfMessageHolder
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class AsfMessageHolder
{
   private JBossMessage msg;
   
   private String consumerID;
   
   private String queueName;
   
   private int maxDeliveries;
   
   private ClientSession connectionConsumerSession;
   
   private boolean shouldAck;
   
   public AsfMessageHolder(JBossMessage msg, String consumerID,
         String queueName, int maxDeliveries,
         ClientSession connectionConsumerSession, boolean shouldAck)
   {
      this.msg = msg;
      this.consumerID = consumerID;
      this.queueName = queueName;
      this.maxDeliveries = maxDeliveries;
      this.connectionConsumerSession = connectionConsumerSession;
      this.shouldAck = shouldAck;
   }

   public JBossMessage getMsg()
   {
      return msg;
   }

   public void setMsg(JBossMessage msg)
   {
      this.msg = msg;
   }

   public String getConsumerID()
   {
      return consumerID;
   }

   public void setConsumerID(String consumerID)
   {
      this.consumerID = consumerID;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public void setQueueName(String queueName)
   {
      this.queueName = queueName;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }

   public void setMaxDeliveries(int maxDeliveries)
   {
      this.maxDeliveries = maxDeliveries;
   }

   public ClientSession getConnectionConsumerSession()
   {
      return connectionConsumerSession;
   }

   public void setConnectionConsumerSession(ClientSession connectionConsumerSession)
   {
      this.connectionConsumerSession = connectionConsumerSession;
   }

   public boolean isShouldAck()
   {
      return shouldAck;
   }

   public void setShouldAck(boolean shouldAck)
   {
      this.shouldAck = shouldAck;
   }

   
}
