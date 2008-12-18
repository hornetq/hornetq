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
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServerInvocationHandler;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.AcceptorControlMBean;
import org.jboss.messaging.core.management.MessageFlowControlMBean;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.RandomUtil;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class MessageFlowControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   private static MessageFlowControlMBean createControl(String name) throws Exception
   {
      MessageFlowControlMBean control = (MessageFlowControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                               ManagementServiceImpl.getMessageFlowObjectName(name),
                                                                                                               MessageFlowControlMBean.class,
                                                                                                               false);
      return control;
   }

   public static MessageFlowConfiguration randomMessageFlowConfigurationWithDiscoveryGroup(String discoveryGroupName)
   {
      return new MessageFlowConfiguration(randomString(),
                                          randomString(),
                                          null,
                                          randomBoolean(),
                                          randomPositiveInt(),
                                          randomPositiveLong(),
                                          null,
                                          randomPositiveLong(),
                                          randomDouble(),
                                          randomPositiveInt(),
                                          randomPositiveInt(),
                                          randomBoolean(),
                                          discoveryGroupName);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(), "231.7.7.7", 2000, randomPositiveLong());
      MessageFlowConfiguration messageFlowConfig = randomMessageFlowConfigurationWithDiscoveryGroup(discoveryGroupConfig.getName());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getMessageFlowConfigurations().add(messageFlowConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessageFlowControlMBean messageFlowControl = createControl(messageFlowConfig.getName());
      assertEquals(messageFlowConfig.getName(), messageFlowControl.getName());
      assertEquals(messageFlowConfig.getDiscoveryGroupName(), messageFlowControl.getDiscoveryGroupName());
   }

   public void testStartStop() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(), "231.7.7.7", 2000, randomPositiveLong());
      MessageFlowConfiguration messageFlowConfig = randomMessageFlowConfigurationWithDiscoveryGroup(discoveryGroupConfig.getName());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getMessageFlowConfigurations().add(messageFlowConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessageFlowControlMBean messageFlowControl = createControl(messageFlowConfig.getName());
      // started by the service
      assertTrue(messageFlowControl.isStarted());

      messageFlowControl.stop();
      assertFalse(messageFlowControl.isStarted());

      messageFlowControl.start();
      assertTrue(messageFlowControl.isStarted());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      if (service != null)
      {
         service.stop();
      }

      super.tearDown();
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
