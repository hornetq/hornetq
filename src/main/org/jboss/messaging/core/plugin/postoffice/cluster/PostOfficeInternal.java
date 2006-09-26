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

import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jgroups.Address;

/**
 * 
 * A PostOfficeInternal
 * 
 * Extension to the ClusteredPostOffice interface that expose extra methods useful to
 * ClusteredRequests
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
interface PostOfficeInternal extends ClusteredPostOffice
{
   void addBindingFromCluster(String nodeId, String queueName, String condition,
                              String filterString, long channelId, boolean durable)
      throws Exception;
   
   void removeBindingFromCluster(String nodeId, String queueName)
      throws Exception;
   
   void handleAddressNodeMapping(Address address, String nodeId)
      throws Exception;
   
   void routeFromCluster(Message message, String routingKey, Map queueNameNodeIdMap) throws Exception;
   
   //void addToQueue(String queueName, List messages) throws Exception;
   
   void asyncSendRequest(ClusterRequest request) throws Exception;
   
   void asyncSendRequest(ClusterRequest request, String nodeId) throws Exception;
   
   Object syncSendRequest(ClusterRequest request, String nodeId, boolean ignoreNoAddress) throws Exception;
   
   void holdTransaction(TransactionId id, ClusterTransaction tx) throws Throwable;
   
   void commitTransaction(TransactionId id) throws Throwable;
   
   void check(String nodeId) throws Throwable;
   
   void updateQueueStats(String nodeId, List stats) throws Exception;
   
   void sendQueueStats() throws Exception;
   
   boolean referenceExistsInStorage(long channelID, long messageID) throws Exception;
   
   List getDeliveries(String queueName, int numMessages) throws Exception; 
}
