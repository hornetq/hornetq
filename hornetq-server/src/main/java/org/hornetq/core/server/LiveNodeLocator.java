/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.server;

import org.hornetq.api.core.HornetQException;
import org.hornetq.utils.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.server.impl.QuorumManager;

/**
 * A class that will locate a particular live server running in a cluster.
 * How this live is chosen is a job for the implementation
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         8/2/12
 */
public abstract class LiveNodeLocator implements ClusterTopologyListener
{
   private QuorumManager quorumManager;

   public LiveNodeLocator(QuorumManager quorumManager)
   {
      this.quorumManager = quorumManager;
   }

   /*
  * locate a possible live server in a cluster
  * */
   public abstract void locateNode() throws HornetQException;

   /*
   * get the current connector
   * */
   public abstract Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration();

   /*
   * get the node id for the current connector
   * */
   public abstract String getNodeID();

   /*
   * tells the locator the the current connector has failed.
   * */
   public  void notifyRegistrationFailed(boolean alreadyReplicating)
   {
      if(alreadyReplicating)
      {
         quorumManager.notifyAlreadyReplicating();
      }
      else
      {
         quorumManager.notifyRegistrationFailed();
      }
   }

   /*
   * connect to the cluster
   * */
   public void connectToCluster(ServerLocatorInternal serverLocator) throws HornetQException
   {
      serverLocator.connect();
   }
}
