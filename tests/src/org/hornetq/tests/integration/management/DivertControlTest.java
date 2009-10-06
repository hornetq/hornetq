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

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.DivertControl;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.utils.SimpleString;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class DivertControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   private DivertConfiguration divertConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(new SimpleString(divertConfig.getName())));
      
      DivertControl divertControl = createManagementControl(divertConfig.getName());

      assertEquals(divertConfig.getFilterString(), divertControl.getFilter());

      assertEquals(divertConfig.isExclusive(), divertControl.isExclusive());

      assertEquals(divertConfig.getName(), divertControl.getUniqueName());

      assertEquals(divertConfig.getRoutingName(), divertControl.getRoutingName());

      assertEquals(divertConfig.getAddress(), divertControl.getAddress());

      assertEquals(divertConfig.getForwardingAddress(), divertControl.getForwardingAddress());

      assertEquals(divertConfig.getTransformerClassName(), divertControl.getTransformerClassName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName());

      QueueConfiguration queueConfig = new QueueConfiguration(randomString(), randomString(), null, false);
      QueueConfiguration fowardQueueConfig = new QueueConfiguration(randomString(), randomString(), null, false);

      divertConfig = new DivertConfiguration(randomString(),
                                             randomString(),
                                             queueConfig.getAddress(),
                                             fowardQueueConfig.getAddress(),
                                             randomBoolean(),
                                             null,
                                             null);
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getQueueConfigurations().add(queueConfig);
      conf.getQueueConfigurations().add(fowardQueueConfig);
      conf.getDivertConfigurations().add(divertConfig);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);

      service = HornetQ.newHornetQServer(conf, mbeanServer, false);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(new SimpleString(divertConfig.getName())));
      
      service = null;
      
      divertConfig = null;

      super.tearDown();
   }

   protected DivertControl createManagementControl(String name) throws Exception
   {
      return ManagementControlHelper.createDivertControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
