/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.core.server;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.server.impl.QuorumManager;

/**
 * A class that will locate a particular live server running in a cluster. How this live is chosen
 * is a job for the implementation
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class LiveNodeLocator implements ClusterTopologyListener
{
   private final QuorumManager quorumManager;

   public LiveNodeLocator(QuorumManager quorumManager)
   {
      this.quorumManager = quorumManager;
   }

   /**
    * Locates a possible live server in a cluster
    */
   public abstract void locateNode() throws HornetQException;

   /**
    * Returns the current connector
    */
   public abstract Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration();

   /**
    * Returns the node id for the current connector
    */
   public abstract String getNodeID();

   /**
    * tells the locator the the current connector has failed.
    */
   public void notifyRegistrationFailed(boolean alreadyReplicating)
   {
      if (alreadyReplicating)
      {
         quorumManager.notifyAlreadyReplicating();
      }
      else
      {
         quorumManager.notifyRegistrationFailed();
      }
   }

   /**
    * connects to the cluster
    */
   public void connectToCluster(ServerLocatorInternal serverLocator) throws HornetQException
   {
      serverLocator.connect();
   }
}
