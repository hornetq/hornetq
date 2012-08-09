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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.server.LiveNodeLocator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This implementation looks for a live server in the cluster with a specific nodeGroupName
 * @see org.hornetq.core.config.Configuration#getNodeGroupName()
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         8/1/12
 */
public class NamedLiveNodeLocator extends LiveNodeLocator
{
   private Lock lock = new ReentrantLock();
   private Condition condition = lock.newCondition();
   private String nodeGroupName;
   private QuorumManager quorumManager;
   private TransportConfiguration liveConfiguration;

   private String nodeID;

   public NamedLiveNodeLocator(String nodeGroupName, QuorumManager quorumManager)
   {
      this.nodeGroupName = nodeGroupName;
      this.quorumManager = quorumManager;
   }

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
   public void nodeUP(long eventUID, String nodeID, String nodeName, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
   {
      try
      {
         lock.lock();
         if(nodeGroupName.equals(nodeName) && connectorPair.getA() != null)
         {
            System.out.println("NamedLiveNodeLocator.nodeUP " + quorumManager + " " + connectorPair.getA());
            liveConfiguration = connectorPair.getA();
            this.nodeID = nodeID;
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

   public String getNodeID()
   {
      return nodeID;
   }

   public TransportConfiguration getLiveConfiguration()
   {
      return liveConfiguration;
   }

   public void notifyRegistrationFailed()
   {
      try
      {
         lock.lock();
         liveConfiguration = null;
         quorumManager.notifyRegistrationFailed();
      }
      finally
      {
         lock.unlock();
      }
   }
}

