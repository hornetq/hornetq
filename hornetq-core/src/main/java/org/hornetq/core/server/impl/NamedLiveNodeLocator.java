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
package org.hornetq.core.server.impl;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.server.LiveNodeLocator;
import org.hornetq.utils.Pair;

/**
 * NamedLiveNodeLocator looks for a live server in the cluster with a specific nodeGroupName
 * @see org.hornetq.core.config.Configuration#getBackupGroupName()
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class NamedLiveNodeLocator extends LiveNodeLocator
{
   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final String nodeGroupName;
   private Pair<TransportConfiguration, TransportConfiguration> liveConfiguration;

   private String nodeID;

   public NamedLiveNodeLocator(String nodeGroupName, QuorumManager quorumManager)
   {
      super(quorumManager);
      this.nodeGroupName = nodeGroupName;
   }

   @Override
   public void locateNode() throws HornetQException
   {
      try
      {
         lock.lock();
         if(liveConfiguration == null)
         {
            try
            {
               condition.await();
            }
            catch (InterruptedException e)
            {
               //ignore
            }
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      try
      {
         lock.lock();
         if (nodeGroupName.equals(topologyMember.getBackupGroupName()) && topologyMember.getLive() != null)
         {
            liveConfiguration =
                     new Pair<TransportConfiguration, TransportConfiguration>(topologyMember.getLive(),
                                                                              topologyMember.getBackup());
            nodeID = topologyMember.getNodeId();
            condition.signal();
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      //no op
   }

   @Override
   public String getNodeID()
   {
      return nodeID;
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration()
   {
      return liveConfiguration;
   }

   @Override
   public void notifyRegistrationFailed(boolean alreadyReplicating)
   {
      try
      {
         lock.lock();
         liveConfiguration = null;
         super.notifyRegistrationFailed(alreadyReplicating);
      }
      finally
      {
         lock.unlock();
      }
   }
}

