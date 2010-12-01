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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A DivertControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class DivertControlUsingCoreTest extends DivertControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlTest overrides --------------------------------

   @Override
   protected DivertControl createManagementControl(final String name) throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = locator.createSessionFactory();
      session = sf.createSession(false, true, true);
      session.start();

      return new DivertControl()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_DIVERT + name);

         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("address");
         }

         public String getFilter()
         {
            return (String)proxy.retrieveAttributeValue("filter");
         }

         public String getForwardingAddress()
         {
            return (String)proxy.retrieveAttributeValue("forwardingAddress");
         }

         public String getRoutingName()
         {
            return (String)proxy.retrieveAttributeValue("routingName");
         }

         public String getTransformerClassName()
         {
            return (String)proxy.retrieveAttributeValue("transformerClassName");
         }

         public String getUniqueName()
         {
            return (String)proxy.retrieveAttributeValue("uniqueName");
         }

         public boolean isExclusive()
         {
            return (Boolean)proxy.retrieveAttributeValue("exclusive");
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

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
