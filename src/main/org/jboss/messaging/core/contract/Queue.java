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
package org.jboss.messaging.core.contract;

import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.impl.clusterconnection.MessageSucker;


/**
 * A Queue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public interface Queue extends Channel
{
   Filter getFilter();
   
   /**
    * Merge the contents of one queue with another - this happens at failover when a queue is failed
    * over to another node, but a queue with the same name already exists. In this case we merge the
    * two queues.
    */
   void mergeIn(long channelID, int nodeID) throws Exception;
   
   /* 
    * TODO - this method does not really belong here - the only reason it is, is because we create the 
    * queues in the post office on startup but paging info is only known at deploy time
    */
   void setPagingParams(int fullSize, int pageSize, int downCacheSize);
   
   int getFullSize();
   
   int getPageSize();
   
   int getDownCacheSize();
   
   boolean isClustered();
   
   String getName();
   
   int getNodeID();
   
   long getRecoverDeliveriesTimeout();
   
   Distributor getLocalDistributor();
   
   Distributor getRemoteDistributor();   
   
   void registerSucker(MessageSucker sucker);
   
   boolean unregisterSucker(MessageSucker sucker);
   
   void addToRecoveryArea(int nodeID, long messageID, String sessionID);
   
   void removeFromRecoveryArea(int nodeID, long messageID);
   
   void removeAllFromRecoveryArea(int nodeID);
   
   void addAllToRecoveryArea(int nodeID, Map ids);
   
   List recoverDeliveries(List messageIds);  
   
   void removeStrandedReferences(String sessionID);
}
