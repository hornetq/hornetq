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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author Clebert Suconic
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{

   private static final long serialVersionUID = -9037171688692471371L;

   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   private static final Logger log = Logger.getLogger(Topology.class);

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

   private transient Map<String, Long> mapDelete;

   public Topology(final Object owner)
   {
      this.owner = owner;
      if (log.isTraceEnabled())
      {
         Topology.log.trace("Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE",
                            new Exception("trace"));
      }
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

   /** This is called by the server when the node is activated from backup state. It will always succeed */
   public void updateAsLive(final String nodeId, final TopologyMember memberInput)
   {
      synchronized (this)
      {
         if (log.isDebugEnabled())
         {
            log.debug(this + "::node " + nodeId + "=" + memberInput);
         }
         memberInput.setUniqueEventID(System.currentTimeMillis());
         topology.remove(nodeId);
         topology.put(nodeId, memberInput);
         sendMemberUp(memberInput.getUniqueEventID(), nodeId, memberInput);
      }
   }

   /** This is called by the server when the node is activated from backup state. It will always succeed */
   public TopologyMember updateBackup(final String nodeId, final TopologyMember memberInput)
   {
      if (log.isTraceEnabled())
      {
         log.trace(this + "::updateBackup::" + nodeId + ", memberInput=" + memberInput);
      }

      synchronized (this)
      {
         TopologyMember currentMember = getMember(nodeId);
         if (currentMember == null)
         {
            log.debug("There's no live to be updated on backup update, node=" + nodeId + " memberInput=" + memberInput,
                     new Exception("trace"));

            currentMember = memberInput;
            topology.put(nodeId, currentMember);
         }

         TopologyMember newMember = new TopologyMember(currentMember.getA(), memberInput.getB());
         newMember.setUniqueEventID(System.currentTimeMillis());
         topology.remove(nodeId);
         topology.put(nodeId, newMember);
         sendMemberUp(newMember.getUniqueEventID(), nodeId, newMember);

         return newMember;
      }
   }

   /**
    * 
    * @param <p>uniqueIdentifier an unique identifier for when the change was made
    *           We will use current time millis for starts, and a ++ of that number for shutdown. </p> 
    * @param nodeId
    * @param memberInput
    * @return
    */
   public boolean updateMember(final long uniqueEventID, final String nodeId, final TopologyMember memberInput)
   {

      Long deleteTme = getMapDelete().get(nodeId);
      if (deleteTme != null && uniqueEventID < deleteTme)
      {
         log.debug("Update uniqueEvent=" + uniqueEventID +
                   ", nodeId=" +
                   nodeId +
                   ", memberInput=" +
                   memberInput +
                   " being rejected as there was a delete done after that");
         return false;
      }

      synchronized (this)
      {
         TopologyMember currentMember = topology.get(nodeId);

         if (currentMember == null)
         {
            if (Topology.log.isDebugEnabled())
            {
               Topology.log.debug(this + "::NewMemeberAdd nodeId=" +
                                  nodeId +
                                  " member = " +
                                  memberInput, new Exception("trace"));
            }
            memberInput.setUniqueEventID(uniqueEventID);
            topology.put(nodeId, memberInput);
            sendMemberUp(uniqueEventID, nodeId, memberInput);
            return true;
         }
         else
         {
            if (uniqueEventID > currentMember.getUniqueEventID())
            {
               TopologyMember newMember =  new TopologyMember(memberInput.getA(), memberInput.getB());

               if (newMember.getA() == null && currentMember.getA() != null)
               {
                  newMember.setA(currentMember.getA());
               }

               if (newMember.getB() == null && currentMember.getB() != null)
               {
                  newMember.setB(currentMember.getB());
               }

               if (log.isDebugEnabled())
               {
                  log.debug(this + "::updated currentMember=nodeID=" +
                            nodeId + 
                            ", currentMember=" +
                            currentMember +
                            ", memberInput=" +
                            memberInput +
                            "newMember=" + newMember, new Exception ("trace"));
               }


               newMember.setUniqueEventID(uniqueEventID);
               topology.remove(nodeId);
               topology.put(nodeId, newMember);
               sendMemberUp(uniqueEventID, nodeId, newMember);

               return true;
            }
            else
            {
               /*always add the backup, better to try to reconnect to something thats not there then to not know about it at all*/
               if(currentMember.getB() == null && memberInput.getB() != null)
               {
                  currentMember.setB(memberInput.getB());
               }
               return false;
            }
         }

      }
   }

   /**
    * @param nodeId
    * @param memberToSend
    */
   private void sendMemberUp(final long uniqueEventID, final String nodeId, final TopologyMember memberToSend)
   {
      final ArrayList<ClusterTopologyListener> copy = copyListeners();

      if (log.isTraceEnabled())
      {
         log.trace(this + "::prepare to send " + nodeId + " to " + copy.size() + " elements");
      }

      if (copy.size() > 0)
      {
         execute(new Runnable()
         {
            public void run()
            {
               for (ClusterTopologyListener listener : copy)
               {
                  if (Topology.log.isTraceEnabled())
                  {
                     Topology.log.trace(Topology.this + " informing " +
                                        listener +
                                        " about node up = " +
                                        nodeId +
                                        " connector = " +
                                        memberToSend.getConnector());
                  }
   
                  try
                  {
                     listener.nodeUP(uniqueEventID, nodeId, memberToSend.getConnector(), false);
                  }
                  catch (Throwable e)
                  {
                     log.warn(e.getMessage(), e);
                  }
               }
            }
         });
      }
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

   public boolean removeMember(final long uniqueEventID, final String nodeId)
   {
      TopologyMember member;

      synchronized (this)
      {
         member = topology.get(nodeId);
         if (member != null)
         {
            if (member.getUniqueEventID() > uniqueEventID)
            {
               log.debug("The removeMember was issued before the node " + nodeId + " was started, ignoring call");
               member = null;
            }
            else
            {
               getMapDelete().put(nodeId, uniqueEventID);
               member = topology.remove(nodeId);
            }
         }
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
                     listener.nodeDown(uniqueEventID, nodeId);
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
    * it will send the member to its listeners
    * @param nodeID
    * @param member
    */
   public void sendMember(final String nodeID)
   {
      final TopologyMember member = getMember(nodeID);

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
               listener.nodeUP(member.getUniqueEventID(), nodeID, member.getConnector(), false);
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

      execute(new Runnable()
      {
         public void run()
         {
            int count = 0;

            final Map<String, TopologyMember> copy;

            synchronized (Topology.this)
            {
               copy = new HashMap<String, TopologyMember>(topology);
            }

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
               listener.nodeUP(entry.getValue().getUniqueEventID(),
                               entry.getKey(),
                               entry.getValue().getConnector(),
                               ++count == copy.size());
            }
         }
      });
   }

   public synchronized TopologyMember getMember(final String nodeID)
   {
      return topology.get(nodeID);
   }

   public synchronized boolean isEmpty()
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
         if (member.getA() != null)
         {
            count++;
         }
         if (member.getB() != null)
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

      String desc = text + "topology on " + this + ":\n";
      for (Entry<String, TopologyMember> entry : new HashMap<String, TopologyMember>(topology).entrySet())
      {
         desc += "\t" + entry.getKey() + " => " + entry.getValue() + "\n";
      }
      desc += "\t" + "nodes=" + nodes() + "\t" + "members=" + members();
      if (topology.isEmpty())
      {
         desc += "\tEmpty";
      }
      return desc;
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

   public TransportConfiguration getBackupForConnector(final TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : topology.values())
      {
         if (member.getA() != null && member.getA().equals(connectorConfiguration))
         {
            return member.getB();
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
         return "Topology@" + Integer.toHexString(System.identityHashCode(this));
      }
      else
      {
         return "Topology@" + Integer.toHexString(System.identityHashCode(this)) + "[owner=" + owner + "]";
      }
   }

   private synchronized Map<String, Long> getMapDelete()
   {
      if (mapDelete == null)
      {
         mapDelete = new ConcurrentHashMap<String, Long>();      
      }
      return mapDelete;
   }

}
