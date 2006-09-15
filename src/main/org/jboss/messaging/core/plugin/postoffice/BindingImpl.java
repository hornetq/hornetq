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

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.selector.Selector;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.util.StreamUtils;

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
public class BindingImpl implements Binding
{
   private String nodeId;
   
   private String queueName;
   
   private String condition;
   
   private Queue queue;
    
   private boolean active;
   
   private Filter filter;
   
   private long channelId;
     
   private boolean durable;
         
   public BindingImpl()
   {      
   }

   public BindingImpl(String nodeId, String queueName, String condition, Filter filter,
                      long channelId, boolean durable)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.condition = condition;      
      
      this.filter = filter;
      
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
   
   public Filter getFilter()
   {
      return filter;
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
   
   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readUTF();
      
      queueName = in.readUTF();
      
      condition = in.readUTF();
      
      active = in.readBoolean();
      
      String filterString = (String)StreamUtils.readObject(in, false);
      
      //TODO we need to generalise this
      filter = new Selector(filterString);
      
      channelId = in.readLong();
      
      durable = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(nodeId);
      
      out.writeUTF(queueName);
      
      out.writeUTF(condition);
      
      out.writeBoolean(active);
      
      StreamUtils.writeObject(out, filter.getFilterString(), false, false);
      
      out.writeLong(channelId);
      
      out.writeBoolean(durable);
   }

}
