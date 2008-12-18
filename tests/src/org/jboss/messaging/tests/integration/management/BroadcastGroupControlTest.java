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

import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServerInvocationHandler;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.BroadcastGroupControlMBean;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.util.Pair;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class BroadcastGroupControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   private static BroadcastGroupControlMBean createControl(String name) throws Exception
   {
      BroadcastGroupControlMBean control = (BroadcastGroupControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                                     ManagementServiceImpl.getBroadcastGroupObjectName(name),
                                                                                                                     BroadcastGroupControlMBean.class,
                                                                                                                     false);
      return control;
   }

   public static BroadcastGroupConfiguration randomBroadcastGroupConfiguration(List<Pair<String, String>> connectorInfos)
   {
      return new BroadcastGroupConfiguration(randomString(),
                                             "localhost",
                                             randomPositiveInt(),
                                             "231.7.7.7",
                                             randomPositiveInt(),
                                             randomPositiveLong(),
                                             connectorInfos);
   }

   public static Pair<String, String> randomPair()
   {
      return new Pair<String, String>(randomString(), randomString());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<Pair<String, String>> connectorInfos = new ArrayList<Pair<String,String>>();
      connectorInfos.add(new Pair<String, String>(connectorConfiguration.getName(), null));
      BroadcastGroupConfiguration broadcastGroupConfig = randomBroadcastGroupConfiguration(connectorInfos);

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      BroadcastGroupControlMBean broadcastGroupControl = createControl(broadcastGroupConfig.getName());

      assertEquals(broadcastGroupConfig.getName(), broadcastGroupControl.getName());
      assertEquals(broadcastGroupConfig.getGroupAddress(), broadcastGroupControl.getGroupAddress());
      assertEquals(broadcastGroupConfig.getGroupPort(), broadcastGroupControl.getGroupPort());
      assertEquals(broadcastGroupConfig.getLocalBindAddress(), broadcastGroupControl.getLocalBindAddress());
      assertEquals(broadcastGroupConfig.getLocalBindPort(), broadcastGroupControl.getLocalBindPort());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<Pair<String, String>> connectorInfos = new ArrayList<Pair<String,String>>();
      connectorInfos.add(new Pair<String, String>(connectorConfiguration.getName(), null));
      BroadcastGroupConfiguration broadcastGroupConfig = randomBroadcastGroupConfiguration(connectorInfos);

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      BroadcastGroupControlMBean broadcastGroupControl = createControl(broadcastGroupConfig.getName());

      // started by the service
      assertTrue(broadcastGroupControl.isStarted());

      broadcastGroupControl.stop();
      assertFalse(broadcastGroupControl.isStarted());

      broadcastGroupControl.start();
      assertTrue(broadcastGroupControl.isStarted());
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
