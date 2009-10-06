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

package org.hornetq.tests.integration.management;

import java.util.Map;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.ClusterConnectionControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;

/**
 * A ClusterConnectionControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ClusterConnectionControlUsingCoreTest extends ClusterConnectionControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // ClusterConnectionControlTest overrides --------------------------------

   @Override
   protected ClusterConnectionControl createManagementControl(final String name) throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();

      return new ClusterConnectionControl()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ResourceNames.CORE_CLUSTER_CONNECTION + name);
         
         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("address");
         }

         public String getDiscoveryGroupName()
         {
            return (String)proxy.retrieveAttributeValue("discoveryGroupName");
         }

         public int getMaxHops()
         {
            return (Integer)proxy.retrieveAttributeValue("maxHops");
         }

         public long getRetryInterval()
         {
            return (Long)proxy.retrieveAttributeValue("retryInterval");
         }

         public Object[] getStaticConnectorNamePairs()
         {
            return (Object[])proxy.retrieveAttributeValue("staticConnectorNamePairs");
         }

         public String getStaticConnectorNamePairsAsJSON()
         {
            return (String)proxy.retrieveAttributeValue("staticConnectorNamePairsAsJSON");
         }
         
         public Map<String, String> getNodes() throws Exception
         {
            return (Map<String, String>)proxy.retrieveAttributeValue("nodes");
         }

         public boolean isDuplicateDetection()
         {
            return (Boolean)proxy.retrieveAttributeValue("duplicateDetection");
         }

         public boolean isForwardWhenNoConsumers()
         {
            return (Boolean)proxy.retrieveAttributeValue("forwardWhenNoConsumers");
         }

         public String getName()
         {
            return (String)proxy.retrieveAttributeValue("name");
         }
         
         public String getNodeID()
         {
            return (String)proxy.retrieveAttributeValue("nodeID");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("started");
         }

         public void start() throws Exception
         {
            proxy.invokeOperation("start");
         }

         public void stop() throws Exception
         {
            proxy.invokeOperation("stop");
         }

      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      if (session != null)
      {
         session.close();
      }
      
      session = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
