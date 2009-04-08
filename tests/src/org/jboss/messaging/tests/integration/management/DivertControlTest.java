/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.DivertControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.utils.SimpleString;

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

   private MessagingServer service;

   private DivertConfiguration divertConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      checkResource(ObjectNames.getDivertObjectName(new SimpleString(divertConfig.getName())));
      
      DivertControlMBean divertControl = createManagementControl(divertConfig.getName());

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

      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);

      service = Messaging.newNullStorageMessagingServer(conf, mbeanServer);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      checkNoResource(ObjectNames.getDivertObjectName(new SimpleString(divertConfig.getName())));

      super.tearDown();
   }

   protected DivertControlMBean createManagementControl(String name) throws Exception
   {
      return ManagementControlHelper.createDivertControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
