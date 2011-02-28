/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.server.cluster;

import java.util.Map;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.server.HornetQComponent;

/**
 * A ClusterConnection
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 23 Jan 2009 14:51:55
 *
 *
 */
public interface ClusterConnection extends HornetQComponent, ClusterTopologyListener
{
   SimpleString getName();

   String getNodeID();

   /**
    * @return a Map of node ID and addresses
    */
   Map<String, String> getNodes();

   void handleReplicatedAddBinding(SimpleString address,
                                   SimpleString uniqueName,
                                   SimpleString routingName,
                                   long queueID,
                                   SimpleString filterString,
                                   SimpleString queueName,
                                   int distance) throws Exception;

   void activate() throws Exception;
   
   TransportConfiguration getConnector();

   // for debug
   String description();

   void nodeAnnounced(String nodeID, Pair<TransportConfiguration,TransportConfiguration> connectorPair);
}
