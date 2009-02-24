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

import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createAcceptorControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.AcceptorControlMBean;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class AcceptorControlTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         randomString());

      MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();

      AcceptorControlMBean acceptorControl = createAcceptorControl(acceptorConfig.getName(), mbeanServer);

      assertEquals(acceptorConfig.getName(), acceptorControl.getName());
      assertEquals(acceptorConfig.getFactoryClassName(), acceptorControl.getFactoryClassName());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(NettyAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         randomString());
      MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();

      AcceptorControlMBean acceptorControl = createAcceptorControl(acceptorConfig.getName(), mbeanServer);

      // started by the service
      assertTrue(acceptorControl.isStarted());

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      ClientSession session = sf.createSession(false, true, true);
      assertNotNull(session);
      session.close();

      acceptorControl.stop();

      assertFalse(acceptorControl.isStarted());
      try
      {
         sf.createSession(false, true, true);
         fail("acceptor must not accept connections when stopped");
      }
      catch (Exception e)
      {
      }

      acceptorControl.start();

      assertTrue(acceptorControl.isStarted());
      session = sf.createSession(false, true, true);
      assertNotNull(session);
      session.close();
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
