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

package org.hornetq.core.management.impl;

import java.util.List;
import java.util.Map;

import javax.management.StandardMBean;

import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.management.ClusterConnectionControl;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.utils.Pair;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A ClusterConnectionControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClusterConnectionControlImpl extends StandardMBean implements ClusterConnectionControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControlImpl(final ClusterConnection clusterConnection,
                                       ClusterConnectionConfiguration configuration) throws Exception
   {
      super(ClusterConnectionControl.class);
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   public String getAddress()
   {
      return configuration.getAddress();
   }

   public String getDiscoveryGroupName()
   {
      return configuration.getDiscoveryGroupName();
   }

   public int getMaxHops()
   {
      return configuration.getMaxHops();
   }

   public String getName()
   {
      return configuration.getName();
   }

   public long getRetryInterval()
   {
      return configuration.getRetryInterval();
   }
   
   public String getNodeID()
   {
      return clusterConnection.getNodeID();
   }

   public Object[] getStaticConnectorNamePairs()
   {
      List<Pair<String, String>> pairs = configuration.getStaticConnectorNamePairs();
      
      if (pairs == null)
      {
         return null;
      }
         
      Object[] ret = new Object[pairs.size()];

      int i = 0;
      for (Pair<String, String> pair : configuration.getStaticConnectorNamePairs())
      {
         String[] opair = new String[2];

         opair[0] = pair.a;
         opair[1] = pair.b != null ? pair.b : null;

         ret[i++] = opair;
      }

      return ret;
   }

   public String getStaticConnectorNamePairsAsJSON() throws Exception
   {
      List<Pair<String, String>> pairs = configuration.getStaticConnectorNamePairs();
      
      if (pairs == null)
      {
         return null;
      }
      
      JSONArray array = new JSONArray();

      for (Pair<String, String> pair : pairs)
      {
         JSONObject p = new JSONObject();
         p.put("a", pair.a);
         p.put("b", pair.b);
         array.put(p);
      }
      return array.toString();
   }

   public boolean isDuplicateDetection()
   {
      return configuration.isDuplicateDetection();
   }

   public boolean isForwardWhenNoConsumers()
   {
      return configuration.isForwardWhenNoConsumers();
   }

   public Map<String, String> getNodes() throws Exception
   {
      return clusterConnection.getNodes();
   }
   
   public boolean isStarted()
   {
      return clusterConnection.isStarted();
   }

   public void start() throws Exception
   {
      clusterConnection.start();
   }

   public void stop() throws Exception
   {
      clusterConnection.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
