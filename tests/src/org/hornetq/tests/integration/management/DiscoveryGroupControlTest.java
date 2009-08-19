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

package org.hornetq.tests.integration.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createDiscoveryGroupControl;
import static org.hornetq.tests.util.RandomUtil.randomPositiveLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.DiscoveryGroupControl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class DiscoveryGroupControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(), "231.7.7.7", 2000, randomPositiveLong());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newMessagingServer(conf, mbeanServer, false);
      service.start();

      DiscoveryGroupControl discoveryGroupControl = createManagementControl(discoveryGroupConfig.getName());

      assertEquals(discoveryGroupConfig.getName(), discoveryGroupControl.getName());
      assertEquals(discoveryGroupConfig.getGroupAddress(), discoveryGroupControl.getGroupAddress());
      assertEquals(discoveryGroupConfig.getGroupPort(), discoveryGroupControl.getGroupPort());
      assertEquals(discoveryGroupConfig.getRefreshTimeout(), discoveryGroupControl.getRefreshTimeout());
   }

   public void testStartStop() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(), "231.7.7.7", 2000, randomPositiveLong());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newMessagingServer(conf, mbeanServer, false);
      service.start();

      DiscoveryGroupControl discoveryGroupControl = createManagementControl(discoveryGroupConfig.getName());

      // started by the server
      assertTrue(discoveryGroupControl.isStarted());

      discoveryGroupControl.stop();
      assertFalse(discoveryGroupControl.isStarted());
      
      discoveryGroupControl.start();
      assertTrue(discoveryGroupControl.isStarted());
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

      service = null;
      
      super.tearDown();
   }
   
   protected DiscoveryGroupControl createManagementControl(String name) throws Exception
   {
      return createDiscoveryGroupControl(name, mbeanServer);
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
