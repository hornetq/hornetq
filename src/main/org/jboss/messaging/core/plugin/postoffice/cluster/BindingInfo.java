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

import org.jboss.messaging.util.StreamUtils;
import org.jboss.messaging.util.Streamable;

/**
 * A BindingInfo
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BindingInfo implements Streamable
{
   private String nodeId;   
   
   private String queueName;   
   
   private String condition;   
   
   private String filterString; 
   
   private long channelId;   
   
   private boolean durable;
   
   BindingInfo()
   {      
   }
   
   BindingInfo(String nodeId, String queueName, String condition, String filterString,
               long channelId, boolean durable)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.condition = condition;
      
      this.filterString = filterString;
      
      this.channelId = channelId;
      
      this.durable = durable;
   }

   public void execute(PostOfficeInternal office) throws Exception
   {
      office.addBindingFromCluster(nodeId, queueName, condition,
                                   filterString, channelId, durable);
      
   }
   
   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readUTF();
      
      queueName = in.readUTF();
      
      condition = in.readUTF();
      
      filterString = (String)StreamUtils.readObject(in, false);   
      
      channelId = in.readLong();
      
      durable = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(nodeId);
      
      out.writeUTF(queueName);
      
      out.writeUTF(condition);
      
      StreamUtils.writeObject(out, filterString, false, false);
      
      out.writeLong(channelId);
      
      out.writeBoolean(durable);
   }

   public long getChannelId()
   {
      return channelId;
   }

   public String getCondition()
   {
      return condition;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public String getNodeId()
   {
      return nodeId;
   }

   public String getQueueName()
   {
      return queueName;
   }
}
