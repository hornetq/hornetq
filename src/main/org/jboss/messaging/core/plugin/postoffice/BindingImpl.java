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
package org.jboss.messaging.core.plugin.postoffice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.Binding;

/**
 * 
 * A BindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BindingImpl implements Binding, Externalizable
{
   private static final long serialVersionUID = -5518552214992031242L;

   private static final byte NULL = 0;
   
   private static final byte NOT_NULL = 1;   

   private String nodeId;
   
   private String queueName;
   
   private String condition;
   
   private Queue queue;
    
   private boolean active;
   
   private String selector;
   
   private long channelId;
     
   private boolean durable;
   
   public BindingImpl()
   {      
   }

   public BindingImpl(String nodeId, String queueName, String condition, String selector,
                      long channelId, boolean durable)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.condition = condition;      
      
      this.selector = selector;
      
      this.channelId = channelId;
         
      //Bindings are always created de-activated
      this.active = false;
      
      this.durable = durable;          
   }
   
   public String getNodeId()
   {
      return nodeId;
   }
     
   public String getQueueName()
   {
      return queueName;
   }
   
   public String getCondition()
   {
      return condition;
   }
   
   public Queue getQueue()
   {
      return queue;
   }
 
   public void activate()
   {
      active = true;
   }
   
   public void deactivate()
   {
      active = false;
   }
   
   public boolean isActive()
   {
      return active;
   }
   
   public String getSelector()
   {
      return selector;
   }
   
   public long getChannelId()
   {
      return channelId;
   }
    
   public boolean isDurable()
   {
      return durable;
   }
   
   public void setQueue(Queue queue)
   {
      this.queue = queue;
      
      if (queue != null)
      {
         this.channelId = queue.getChannelID();
      }            
   }
   

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      nodeId = in.readUTF();
      
      queueName = in.readUTF();
      
      condition = in.readUTF();
      
      active = in.readBoolean();
      
      selector = readString(in);
      
      channelId = in.readLong();
      
      durable = in.readBoolean();
   }

   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeUTF(nodeId);
      
      out.writeUTF(queueName);
      
      out.writeUTF(condition);
      
      out.writeBoolean(active);
      
      writeString(selector, out);
      
      out.writeLong(channelId);
      
      out.writeBoolean(durable);
   }
   
   private void writeString(String string, DataOutput out) throws IOException
   {
      if (string == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         out.writeByte(NOT_NULL);
         out.writeUTF(string);
      }
   }
   
   private String readString(DataInput in) throws IOException
   {
      byte b = in.readByte();
      
      if (b == NULL)
      {
         return null;
      }
      else
      {
         return in.readUTF();
      }
   }
   
}
