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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Aug 16, 2010
 */
public class Topology implements Serializable
{
   /**
    * 
    */
   private static final long serialVersionUID = -9037171688692471371L;
   /*
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private Map<String, TopologyMember> topology = new HashMap<String, TopologyMember>();

   private boolean debug;

   public synchronized boolean addMember(String nodeId, TopologyMember member)
   {
      boolean replaced = false;
      TopologyMember currentMember = topology.get(nodeId);
      if (debug)
      {
         //System.out.println("member.getConnector() = " + member.getConnector());
      }
      if(currentMember == null)
      {
         topology.put(nodeId, member);
         replaced = true;
      }
      else
      {
         if(hasChanged(currentMember.getConnector().a, member.getConnector().a) && member.getConnector().a != null)
         {
            currentMember.getConnector().a =  member.getConnector().a;
            replaced = true;
         }
         if(hasChanged(currentMember.getConnector().b, member.getConnector().b) && member.getConnector().b != null)
         {
            currentMember.getConnector().b =  member.getConnector().b;
            replaced = true;
         }

         if(member.getConnector().a == null)
         {
            member.getConnector().a = currentMember.getConnector().a;
         }
         if(member.getConnector().b == null)
         {
            member.getConnector().b = currentMember.getConnector().b;
         }
      }
      return replaced;
   }

   public synchronized boolean removeMember(String nodeId)
   {
      TopologyMember member = topology.remove(nodeId);
      return (member != null);
   }

   public synchronized void fireListeners(ClusterTopologyListener listener)
   {
      int count = 0;
      for (Map.Entry<String, TopologyMember> entry : topology.entrySet())
      {
         listener.nodeUP(entry.getKey(), entry.getValue().getConnector(), ++count == topology.size(), entry.getValue().getDistance());
      }
   }

   public TopologyMember getMember(String nodeID)
   {
      return topology.get(nodeID);
   }

   public boolean isEmpty()
   {
      return topology.isEmpty();
   }

   public Collection<TopologyMember> getMembers()
   {
      return topology.values();
   }

   public int nodes()
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

   public String describe()
   {

      String desc = "";
      for (Entry<String, TopologyMember> entry : new HashMap<String, TopologyMember>(topology).entrySet())
      {
         desc += "\t" + entry.getKey() + " => " + entry.getValue() + "\n";
      }
      desc += "\t" + "nodes=" + nodes() + "\t" + "members=" + members();
      return desc;
   }

   public void clear()
   {
      topology.clear();
   }

   public int members()
   {
      return topology.size();
   }

   private boolean hasChanged(TransportConfiguration currentConnector, TransportConfiguration connector)
   {
      return (currentConnector == null && connector != null) || (currentConnector != null && !currentConnector.equals(connector));
   }

   public TransportConfiguration getBackupForConnector(TransportConfiguration connectorConfiguration)
   {
      for (TopologyMember member : topology.values())
      {
         if(member.getConnector().a != null && member.getConnector().a.equals(connectorConfiguration))
         {
            return member.getConnector().b;  
         }
      }
      return null;
   }

   public void setDebug(boolean b)
   {
      debug = b;
   }
}
