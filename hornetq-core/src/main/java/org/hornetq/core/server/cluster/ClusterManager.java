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
import java.util.Set;

import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.server.HornetQComponent;

/**
 * A ClusterManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 18 Nov 2008 09:23:26
 *
 */
public interface ClusterManager extends HornetQComponent
{
   Map<String, Bridge> getBridges();

   Set<ClusterConnection> getClusterConnections();

   /**
    * Return the default ClusterConnection to be used case it's not defined by the acceptor
    * @return
    */
   ClusterConnection getDefaultConnection();

   ClusterConnection getClusterConnection(String name);

   Set<BroadcastGroup> getBroadcastGroups();

   void activate();

   void flushExecutor();

   void announceBackup() throws Exception;

   void deploy() throws Exception;

   void deployBridge(BridgeConfiguration config, boolean start) throws Exception;

   // HORNETQ-720
   void announceReplicatingBackup(Channel liveChannel);

   void destroyBridge(String name) throws Exception;

   /**
    * @return
    */
   String describe();
}
