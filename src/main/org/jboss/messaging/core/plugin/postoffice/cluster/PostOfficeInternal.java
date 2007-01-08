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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;

/**
 * 
 * A PostOfficeInternal
 * 
 * Extension to the ClusteredPostOffice interface that expose extra methods useful to
 * ClusteredRequests
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
interface PostOfficeInternal extends ClusteredPostOffice
{
   void addBindingFromCluster(int nodeId, String queueName, String conditionText,
                              String filterString, long channelId, boolean durable, boolean failed)
      throws Exception;
   
   void removeBindingFromCluster(int nodeId, String queueName)
      throws Exception;
 
   void handleNodeLeft(int nodeId) throws Exception;
   
   void putReplicantLocally(int nodeId, Serializable key, Serializable replicant) throws Exception;
   
   boolean removeReplicantLocally(int nodeId, Serializable key) throws Exception;
   
   void routeFromCluster(Message message, String routingKeyText, Map queueNameNodeIdMap) throws Exception;
   
   void asyncSendRequest(ClusterRequest request) throws Exception;
   
   void asyncSendRequest(ClusterRequest request, int nodeId) throws Exception;
   
   void holdTransaction(TransactionId id, ClusterTransaction tx) throws Throwable;
   
   void commitTransaction(TransactionId id) throws Throwable;
   
   void rollbackTransaction(TransactionId id) throws Throwable;
   
   void updateQueueStats(int nodeId, List stats) throws Exception;
   
   void sendQueueStats() throws Exception;
   
   boolean referenceExistsInStorage(long channelID, long messageID) throws Exception;
   
   void handleMessagePullResult(int remoteNodeId, long holdingTxId, String queueName, Message message) throws Throwable;
}
