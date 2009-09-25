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

import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.management.BridgeControl;
import org.hornetq.core.server.cluster.Bridge;

/**
 * A BridgeControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class BridgeControlImpl extends StandardMBean implements BridgeControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Bridge bridge;

   private final BridgeConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BridgeControlImpl(final Bridge bridge, final BridgeConfiguration configuration) throws Exception
   {
      super(BridgeControl.class);
      this.bridge = bridge;
      this.configuration = configuration;
   }

   // BridgeControlMBean implementation ---------------------------

   public String[] getConnectorPair() throws Exception
   {
      String[] pair = new String[2];

      pair[0] = configuration.getConnectorPair().a;
      pair[1] = configuration.getConnectorPair().b != null ? configuration.getConnectorPair().b : null;

      return pair;
   }

   public String getForwardingAddress()
   {
      return configuration.getForwardingAddress();
   }

   public String getQueueName()
   {
      return configuration.getQueueName();
   }

   public String getDiscoveryGroupName()
   {
      return configuration.getDiscoveryGroupName();
   }

   public String getFilterString()
   {
      return configuration.getFilterString();
   }

   public int getReconnectAttempts()
   {
      return configuration.getReconnectAttempts();
   }

   public boolean isFailoverOnServerShutdown()
   {
      return configuration.isFailoverOnServerShutdown();
   }

   public String getName()
   {
      return configuration.getName();
   }

   public long getRetryInterval()
   {
      return configuration.getRetryInterval();
   }

   public double getRetryIntervalMultiplier()
   {
      return configuration.getRetryIntervalMultiplier();
   }

   public String getTransformerClassName()
   {
      return configuration.getTransformerClassName();
   }

   public boolean isStarted()
   {
      return bridge.isStarted();
   }

   public boolean isUseDuplicateDetection()
   {
      return configuration.isUseDuplicateDetection();
   }

   public void start() throws Exception
   {
      bridge.start();
   }

   public void stop() throws Exception
   {
      bridge.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
