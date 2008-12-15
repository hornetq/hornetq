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

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.TransportConfigurationInfo;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 * 
 * Created 26 nov. 2008 14:18:48
 *
 *
 */
public class MessagingServerControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   private static MessagingServerControlMBean createServerControl() throws Exception
   {
      MessagingServerControlMBean control = (MessagingServerControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                                       ManagementServiceImpl.getMessagingServerObjectName(),
                                                                                                                       MessagingServerControlMBean.class,
                                                                                                                       false);
      return control;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetConnectors() throws Exception
   {
      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData acceptorData = serverControl.getConnectors();
      assertNotNull(acceptorData);
      assertEquals(1, acceptorData.size());

      TransportConfiguration[] connectorConfigurations = TransportConfigurationInfo.from(acceptorData);
      assertEquals(1, connectorConfigurations.length);

      assertEquals(connectorConfig.getName(), connectorConfigurations[0].getName());
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
