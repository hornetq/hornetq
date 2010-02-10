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

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A ClusterConnectionControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClusterConnectionControlImpl extends AbstractControl implements ClusterConnectionControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControlImpl(final ClusterConnection clusterConnection,
                                       final StorageManager storageManager,
                                       final ClusterConnectionConfiguration configuration) throws Exception
   {
      super(ClusterConnectionControl.class, storageManager);
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   public String getAddress()
   {
      clearIO();
      try
      {
         return configuration.getAddress();
      }
      finally
      {
         blockOnIO();
      }

   }

   public String getDiscoveryGroupName()
   {
      clearIO();
      try
      {
         return configuration.getDiscoveryGroupName();
      }
      finally
      {
         blockOnIO();
      }

   }

   public int getMaxHops()
   {
      clearIO();
      try
      {
         return configuration.getMaxHops();
      }
      finally
      {
         blockOnIO();
      }

   }

   public String getName()
   {
      clearIO();
      try
      {
         return configuration.getName();
      }
      finally
      {
         blockOnIO();
      }

   }

   public long getRetryInterval()
   {
      clearIO();
      try
      {
         return configuration.getRetryInterval();
      }
      finally
      {
         blockOnIO();
      }

   }

   public String getNodeID()
   {
      clearIO();
      try
      {
         return clusterConnection.getNodeID();
      }
      finally
      {
         blockOnIO();
      }
   }

   public Object[] getStaticConnectorNamePairs()
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public String getStaticConnectorNamePairsAsJSON() throws Exception
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public boolean isDuplicateDetection()
   {
      clearIO();
      try
      {
         return configuration.isDuplicateDetection();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isForwardWhenNoConsumers()
   {
      clearIO();
      try
      {
         return configuration.isForwardWhenNoConsumers();
      }
      finally
      {
         blockOnIO();
      }
   }

   public Map<String, String> getNodes() throws Exception
   {
      clearIO();
      try
      {
         return clusterConnection.getNodes();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return clusterConnection.isStarted();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void start() throws Exception
   {
      clearIO();
      try
      {
         clusterConnection.start();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void stop() throws Exception
   {
      clearIO();
      try
      {
         clusterConnection.stop();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(ClusterConnectionControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
