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
class BindingInfo implements Streamable
{
   private int nodeId;   
   
   private String queueName;   
   
   private String conditionText;   
   
   private String filterString; 
   
   private long channelId;   
   
   private boolean durable;

   private boolean failed;
   
   BindingInfo()
   {      
   }
   
   BindingInfo(int nodeId, String queueName, String conditionText, String filterString,
               long channelId, boolean durable, boolean failed)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.conditionText = conditionText;
      
      this.filterString = filterString;
      
      this.channelId = channelId;
      
      this.durable = durable;

      this.failed = failed;
   }

   public void execute(PostOfficeInternal office) throws Exception
   {
      office.addBindingFromCluster(nodeId, queueName, conditionText,
                                   filterString, channelId, durable, failed);
      
   }
   
   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readInt();
      
      queueName = in.readUTF();
      
      conditionText = in.readUTF();
      
      filterString = (String)StreamUtils.readObject(in, false);   
      
      channelId = in.readLong();
      
      durable = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(nodeId);
      
      out.writeUTF(queueName);
      
      out.writeUTF(conditionText);
      
      StreamUtils.writeObject(out, filterString, false, false);
      
      out.writeLong(channelId);
      
      out.writeBoolean(durable);
   }

   long getChannelId()
   {
      return channelId;
   }

   String getConditionText()
   {
      return conditionText;
   }

   boolean isDurable()
   {
      return durable;
   }

   String getFilterString()
   {
      return filterString;
   }

   int getNodeId()
   {
      return nodeId;
   }

   String getQueueName()
   {
      return queueName;
   }

   public boolean isFailed()
   {
      return failed;
   }

   public void setFailed(boolean failed)
   {
      this.failed = failed;
   }
}
