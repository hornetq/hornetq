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

import javax.management.StandardMBean;

import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.management.BroadcastGroupControl;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.utils.Pair;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A BroadcastGroupControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class BroadcastGroupControlImpl extends StandardMBean implements BroadcastGroupControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final BroadcastGroup broadcastGroup;

   private final BroadcastGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BroadcastGroupControlImpl(final BroadcastGroup broadcastGroup, final BroadcastGroupConfiguration configuration)
      throws Exception
   {
      super(BroadcastGroupControl.class);
      this.broadcastGroup = broadcastGroup;
      this.configuration = configuration;
   }

   // BroadcastGroupControlMBean implementation ---------------------
   
   public String getName()
   {
      return configuration.getName();
   }

   public long getBroadcastPeriod()
   {
      return configuration.getBroadcastPeriod();
   }

   public Object[] getConnectorPairs()
   {
      Object[] ret = new Object[configuration.getConnectorInfos().size()];
      
      int i = 0;
      for (Pair<String, String> pair: configuration.getConnectorInfos())
      {
         String[] opair = new String[2];
         
         opair[0] = pair.a;
         opair[1] = pair.b != null ? pair.b : null;
         
         ret[i++] = opair;
      }
      
      return ret;      
   }
   
   public String getConnectorPairsAsJSON() throws Exception
   {
      JSONArray array = new JSONArray();
      
      for (Pair<String, String> pair: configuration.getConnectorInfos())
      {
         JSONObject p = new JSONObject();
         p.put("a", pair.a);
         p.put("b", pair.b);
         array.put(p);
      }
      return array.toString();
   }

   public String getGroupAddress()
   {
      return configuration.getGroupAddress();
   }

   public int getGroupPort()
   {
      return configuration.getGroupPort();
   }

   public int getLocalBindPort()
   {
      return configuration.getLocalBindPort();
   }

   // MessagingComponentControlMBean implementation -----------------

   public boolean isStarted()
   {
      return broadcastGroup.isStarted();
   }

   public void start() throws Exception
   {
      broadcastGroup.start();
   }

   public void stop() throws Exception
   {
      broadcastGroup.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
