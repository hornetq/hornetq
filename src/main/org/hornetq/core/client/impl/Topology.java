/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.client.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{

   private static final int BACKOF_TIMEOUT = 50;

   private static final long serialVersionUID = -9037171688692471371L;

   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   private static final Logger log = Logger.getLogger(Topology.class);

   private transient HashMap<String, Pair<Long, Integer>> mapBackof = new HashMap<String, Pair<Long, Integer>>();

   private Executor executor = null;

   /** Used to debug operations.
    * 
    *  Someone may argue this is not needed. But it's impossible to debg anything related to topology without knowing what node
    *  or what object missed a Topology update.
    *  
    *  Hence I added some information to locate debugging here. 
    *  */
   private volatile Object owner;

   /**
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private final Map<String, TopologyMember> topology = new ConcurrentHashMap<String, TopologyMember>();

   public Topology(final Object owner)
   {
      this.owner = owner;
      Topology.log.debug("Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE",
                         new Exception("trace")); // Delete this line
   }

   public void setExecutor(final Executor executor)
   {
      this.executor = executor;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::Adding topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners)
      {
         topologyListeners.add(listener);
      }
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::Removing topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners)
      {
         topologyListeners.remove(listener);
      }
   }

   public boolean addMember(final String nodeId, final TopologyMember member, final boolean last)
   {
      boolean replaced = false;

      synchronized (this)
      {
         TopologyMember currentMember = topology.get(nodeId);

         if (Topology.log.isDebugEnabled())
         {
            Topology.log.debug(this + "::adding = " + nodeId + ":" + member.getConnector(), new Exception("trace"));
         }

         if (currentMember == null)
         {
            if (!testBackof(nodeId))
            {
               return false;
            }

            replaced = true;
            if (Topology.log.isDebugEnabled())
            {
               Topology.log.debug("Add " + this +
                                  " MEMBER WAS NULL, Add member nodeId=" +
                                  nodeId +
                                  " member = " +
                                  member +
                                  " replaced = " +
                                  replaced +
                                  " size = " +
                                  topology.size(), new Exception("trace"));
            }
            topology.put(nodeId, member);
         }
         else
         {
            if (hasChanged(currentMember.getConnector().a, member.getConnector().a) && member.getConnector().a != null)
            {
               if (!testBackof(nodeId))
               {
                  return false;
               }

               currentMember.getConnector().a = member.getConnector().a;
               replaced = true;
            }
            if (hasChanged(currentMember.getConnector().b, member.getConnector().b) && member.getConnector().b != null)
            {
               if (!testBackof(nodeId))
               {
                  return false;
               }

               currentMember.getConnector().b = member.getConnector().b;
               replaced = true;
            }

            if (member.getConnector().a == null)
            {
               member.getConnector().a = currentMember.getConnector().a;
            }
            if (member.getConnector().b == null)
            {
               member.getConnector().b = currentMember.getConnector().b;
            }
         }

         if (Topology.log.isDebugEnabled())
         {
            Topology.log.debug(this + " Add member nodeId=" +
                               nodeId +
                               " member = " +
                               member +
                               " replaced = " +
                               replaced +
                               " size = " +
                               topology.size(), new Exception("trace"));
         }

      }

      if (replaced)
      {

         final ArrayList<ClusterTopologyListener> copy = copyListeners();

         execute(new Runnable()
         {
            public void run()
            {
               for (ClusterTopologyListener listener : copy)
               {
                  if (Topology.log.isTraceEnabled())
                  {
                     Topology.log.trace(this + " informing " + listener + " about node up = " + nodeId);
                  }

                  try
                  {
                     listener.nodeUP(nodeId, member.getConnector(), last);
                  }
                  catch (Throwable e)
                  {
                     log.warn(e.getMessage(), e);
                  }
               }
            }
         });
      }

      return replaced;
   }

   /**
    * @param nodeId
    * @param backOfData
    */
   private boolean testBackof(final String nodeId)
   {
      Pair<Long, Integer> backOfData = mapBackof.get(nodeId);

      if (backOfData != null)
      {
         backOfData.b += 1;
         
         long timeDiff = System.currentTimeMillis() - backOfData.a;

         // To prevent a loop where nodes are being considered down and up
         if (backOfData.b > 5 && timeDiff < BACKOF_TIMEOUT)
         {
            // The cluster may get in loop without this..
            // Case one node is stll sending nodeDown while another member is sending nodeUp
            log.warn("The topology controller identified a blast of nodeUp and down and it is ignoring a nodeUP",
                     new Exception("this exception is just to trace location"));
            return false;
         }
         else if (timeDiff >= BACKOF_TIMEOUT)
         {
            mapBackof.remove(nodeId);
         }
      }

      return true;
   }

   /**
    * @return
    */
   private ArrayList<ClusterTopologyListener> copyListeners()
   {
      ArrayList<ClusterTopologyListener> listenersCopy;
      synchronized (topologyListeners)
      {
         listenersCopy = new ArrayList<ClusterTopologyListener>(topologyListeners);
      }
      return listenersCopy;
   }

   public boolean removeMember(final String nodeId)
   {
      TopologyMember member;

      synchronized (this)
      {
         Pair<Long, Integer> value = mapBackof.get(nodeId);

         if (value == null)
         {
            value = new Pair<Long, Integer>(0l, 0);
            mapBackof.put(nodeId, value);
         }

         value.a = System.currentTimeMillis();

         if (System.currentTimeMillis() - value.a > BACKOF_TIMEOUT)
         {
            value.b = 0;
         }

         member = topology.remove(nodeId);
      }

      if (Topology.log.isDebugEnabled())
      {
         Topology.log.debug("removeMember " + this +
                            " removing nodeID=" +
                            nodeId +
                            ", result=" +
                            member +
                            ", size = " +
                            topology.size(), new Exception("trace"));
      }

      if (member != null)
      {
         final ArrayList<ClusterTopologyListener> copy = copyListeners();

         execute(new Runnable()
         {
            public void run()
            {
               for (ClusterTopologyListener listener : copy)
               {
                  if (Topology.log.isTraceEnabled())
                  {
                     Topology.log.trace(this + " informing " + listener + " about node down = " + nodeId);
                  }
                  try
                  {
                     listener.nodeDown(nodeId);
                  }
                  catch (Exception e)
                  {
                     log.warn(e.getMessage(), e);
                  }
               }
            }
         });

      }
      return member != null;
   }

   protected void execute(final Runnable runnable)
   {
      if (executor != null)
      {
         executor.execute(runnable);
      }
      else
      {
         runnable.run();
      }
   }

   /**
    * it will send all the member updates to listeners, independently of being changed or not
    * @param nodeID
    * @param member
    */
   public void sendMemberToListeners(final String nodeID, final TopologyMember member)
   {
      // To make sure it was updated
      addMember(nodeID, member, false);

      final ArrayList<ClusterTopologyListener> copy = copyListeners();

      execute(new Runnable()
      {
         public void run()
         {
            // Now force sending it
            for (ClusterTopologyListener listener : copy)
            {
               if (log.isDebugEnabled())
               {
                  log.debug("Informing client listener " + listener +
                            " about itself node " +
                            nodeID +
                            " with connector=" +
                            member.getConnector());
               }
               listener.nodeUP(nodeID, member.getConnector(), false);
            }
         }
      });
   }

   public synchronized void sendTopology(final ClusterTopologyListener listener)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + " is sending topology to " + listener);
      }

      final Map<String, TopologyMember> copy;

      synchronized (this)
      {
         copy = new HashMap<String, TopologyMember>(topology);
      }

      execute(new Runnable()
      {
         public void run()
         {
            int count = 0;

            for (Map.Entry<String, TopologyMember> entry : copy.entrySet())
            {
               if (log.isDebugEnabled())
               {
                  log.debug(Topology.this + " sending " +
                            entry.getKey() +
                            " / " +
                            entry.getValue().getConnector() +
                            " to " +
                            listener);
               }
               listener.nodeUP(entry.getKey(), entry.getValue().getConnector(), ++count == copy.size());
            }
         }
      });
   }

   public TopologyMember getMember(final String nodeID)
   {
      return topology.get(nodeID);
   }

   public boolean isEmpty()
   {
      return topology.isEmpty();
   }

   public Collection<TopologyMember> getMembers()
   {
      ArrayList<TopologyMember> members;
      synchronized (this)
      {
         members = new ArrayList<TopologyMember>(topology.values());
      }
      return members;
   }

   public synchronized int nodes()
   {
      int count = 0;
      for (TopologyMember member : topology.values())
      {
         if (member.getConnector().a != null)
         {
            count++;
         }
         if (member.getConnector().b != null)
         {
            count++;
         }
      }
      return count;
   }

   public synchronized String describe()
   {
      return describe("");
   }

   public synchronized String describe(final String text)
   {

      String desc = text + "\n";
      for (Entry<String, TopologyMember> entry : new HashMap<String, TopologyMember>(topology).entrySet())
      {
         desc += "\t" + entry.getKey() + " => " + entry.getValue() + "\n";
      }
      desc += "\t" + "nodes=" + nodes() + "\t" + "members=" + members();
      return desc;
   }

   public void clear()
   {
      if (Topology.log.isDebugEnabled())
      {
         Topology.log.debug(this + "::clear", new Exception("trace"));
      }
      topology.clear();
   }

   public int members()
   {
      return topology.size();
   }

   /** The owner exists mainly for debug purposes.
    *  When enabling logging and tracing, the Topology updates will include the owner, what will enable to identify
    *  what instances are receiving the updates, what will enable better debugging.*/
   public void setOwner(final Object owner)
   {
      this.owner = owner;
   }

   private boolean hasChanged(final TransportConfiguration currentConnector, final TransportConfiguration connector)
   {
      return currentConnector == null && connector != null ||
             currentConnector != null &&
             !currentConnector.equals(connector);
   }

   public TransportConfiguration getBackupForConnector(final TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : topology.values())
      {
         if (member.getConnector().a != null && member.getConnector().a.equals(connectorConfiguration))
         {
            return member.getConnector().b;
         }
      }
      return null;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      if (owner == null)
      {
         return super.toString();
      }
      else
      {
         return "Topology@" + Integer.toHexString(System.identityHashCode(this)) + "[owner=" + owner + "]";
      }
   }

}
