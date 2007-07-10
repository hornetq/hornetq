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
package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.StreamUtils;
import org.jboss.messaging.util.Streamable;

/**
 * 
 * A MappingInfo
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision: 2421 $</tt>
 *
 * $Id: DefaultBinding.java 2421 2007-02-25 00:06:06Z timfox $
 *
 */
class MappingInfo implements Streamable
{
	private int nodeId;   
   
   private String conditionText;   
   
   private String filterString; 
   
   private long channelId;   
   
   private boolean recoverable;
   
   private int maxSize;
   
   private String queueName;
   
   private boolean clustered;
   
   private int fullSize;
   
   //We need the following attributes too when binding on ALL nodes of the cluster
   private int pageSize;
   
   private int downCacheSize;
   
   private boolean allNodes;
   
   private long recoverDeliveriesTimeout;
   
   MappingInfo()
   {      
   }
   
   MappingInfo(int nodeId, String queueName, String conditionText, String filterString,
               long channelId, boolean recoverable, boolean clustered, boolean allNodes)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.conditionText = conditionText;
      
      this.filterString = filterString;
      
      this.channelId = channelId;
      
      this.recoverable = recoverable;
        
      this.clustered = clustered;
      
      this.allNodes = allNodes;
   }   
   
   MappingInfo(int nodeId, String queueName, String conditionText, String filterString,
   		      long channelId, boolean recoverable, boolean clustered, boolean allNodes,
   		      int fullSize, int pageSize, int downCacheSize,
   		      int maxSize, long recoverDeliveriesTimeout)
   {
   	this (nodeId, queueName, conditionText, filterString, channelId, recoverable, clustered, allNodes);
   	
   	this.fullSize = fullSize;
   	
   	this.pageSize = pageSize;
   	
   	this.downCacheSize = downCacheSize;
   	
   	this.maxSize = maxSize;
   	
   	this.recoverDeliveriesTimeout = recoverDeliveriesTimeout;
   }   
	
	// Streamable implementation ---------------------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readInt();
      
      queueName = in.readUTF();
      
      conditionText = in.readUTF();
      
      filterString = (String)StreamUtils.readObject(in, false);   
      
      channelId = in.readLong();
      
      recoverable = in.readBoolean();
      
      clustered = in.readBoolean();
      
      allNodes = in.readBoolean();
      
      fullSize = in.readInt();
      
      pageSize = in.readInt();
      
      downCacheSize = in.readInt();
      
      maxSize = in.readInt();
      
      recoverDeliveriesTimeout = in.readLong();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(nodeId);
      
      out.writeUTF(queueName);
      
      out.writeUTF(conditionText);
      
      StreamUtils.writeObject(out, filterString, false, false);
      
      out.writeLong(channelId);
      
      out.writeBoolean(recoverable);
      
      out.writeBoolean(clustered);
      
      out.writeBoolean(allNodes);
      
      out.writeInt(fullSize);
      
      out.writeInt(pageSize);
      
      out.writeInt(downCacheSize);
      
      out.writeInt(maxSize);
      
      out.writeLong(recoverDeliveriesTimeout);
   }
   
   int getNodeId()
   {
      return nodeId;
   }
   
   String getQueueName()
   {
   	return queueName;
   }
   
   long getChannelId()
   {
      return channelId;
   }

   String getConditionText()
   {
      return conditionText;
   }

   boolean isRecoverable()
   {
      return recoverable;
   }

   String getFilterString()
   {
      return filterString;
   }
   
   boolean isClustered()
   {
   	return clustered;
   }
   
   boolean isAllNodes()
   {
   	return allNodes;
   }

   int getFullSize()
   {
   	return fullSize;
   }
   
   int getPageSize()
   {
   	return pageSize;   	
   }
   
   int getDownCacheSize()
   {
   	return downCacheSize;
   }
   
   int getMaxSize()
   {
   	return maxSize;
   }
   
   long getRecoverDeliveriesTimeout()
   {
   	return recoverDeliveriesTimeout;
   }

}
