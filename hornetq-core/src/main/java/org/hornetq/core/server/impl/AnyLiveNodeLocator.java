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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This implementatiuon looks for any available live node, once tried with no success it is marked as tried and the next
 * available is used.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         8/1/12
 */
public class AnyLiveNodeLocator extends LiveNodeLocator
{
   private Lock lock = new ReentrantLock();
   private Condition condition = lock.newCondition();
   private QuorumManager quorumManager;
   Map<String, TransportConfiguration> untriedConnectors = new HashMap<String, TransportConfiguration>();
   Map<String, TransportConfiguration> triedConnectors = new HashMap<String, TransportConfiguration>();

   private String nodeID;

   public AnyLiveNodeLocator(QuorumManager quorumManager)
   {
      this.quorumManager = quorumManager;
   }

   public void locateNode() throws HornetQException
   {
      //first time
      try
      {
         lock.lock();
         if(untriedConnectors.isEmpty())
         {
            try
            {
               condition.await();
            }
            catch (InterruptedException e)
            {

            }
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   /*
   *
   * */
   public void nodeUP(long eventUID, String nodeID, String nodeName, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
   {
      try
      {
         lock.lock();
         untriedConnectors.put(nodeID, connectorPair.getA());
         condition.signal();
      }
      finally
      {
         lock.unlock();
      }
   }

   /*
   * if a node goes down we try all the connectors again as one may now be available for replication
   * //todo there will be a better way to do this by finding which nodes backup has gone down.
   * */
   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      try
      {
         lock.lock();
         untriedConnectors.putAll(triedConnectors);
         triedConnectors.clear();
         if(untriedConnectors.size() > 0)
         {
            condition.signal();
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   public String getNodeID()
   {
      return nodeID;
   }

   public TransportConfiguration getLiveConfiguration()
   {
      try
      {
         lock.lock();
         Iterator<String> iterator = untriedConnectors.keySet().iterator();
         //sanity check but this should never happen
         if(iterator.hasNext())
         {
            nodeID = iterator.next();
         }
         return untriedConnectors.get(nodeID);
      }
      finally
      {
         lock.unlock();
      }
   }

   public void notifyRegistrationFailed()
   {
      try
      {
         lock.lock();
         TransportConfiguration tc = untriedConnectors.remove(nodeID);
         //it may have been removed
         if (tc != null)
         {
            triedConnectors.put(nodeID, tc);
         }
      }
      finally
      {
         lock.unlock();
      }
      quorumManager.notifyRegistrationFailed();
   }
}

