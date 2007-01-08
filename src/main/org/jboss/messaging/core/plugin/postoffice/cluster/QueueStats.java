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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * A QueueStats
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class QueueStats implements Streamable
{
   private String queueName;
   
   private int messageCount;

   public QueueStats()
   {      
   }
   
   //QueueStats(String queueName, float addRate, float consumeRate, int messageCount)
   public QueueStats(String queueName, int messageCount)
   {
      this.queueName = queueName;
        
      this.messageCount = messageCount;
   }

   int getMessageCount()
   {
      return messageCount;
   }

   String getQueueName()
   {
      return queueName;
   }

   public void read(DataInputStream in) throws Exception
   {
      queueName = in.readUTF();
      
      messageCount = in.readInt();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(queueName);
        
      out.writeInt(messageCount);
   }     
   
   public String toString()
   {
      return "QueueStats[" + System.identityHashCode(this) + "] queueName: " + queueName + " messageCount: " + messageCount;
   }
}
