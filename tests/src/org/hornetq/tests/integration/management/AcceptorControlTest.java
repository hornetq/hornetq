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

import java.util.HashMap;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.AcceptorControl;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.management.Notification;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class AcceptorControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AcceptorControlTest.class);

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         RandomUtil.randomString());

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      Assert.assertEquals(acceptorConfig.getName(), acceptorControl.getName());
      Assert.assertEquals(acceptorConfig.getFactoryClassName(), acceptorControl.getFactoryClassName());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         RandomUtil.randomString());
      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      // started by the server
      Assert.assertTrue(acceptorControl.isStarted());
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();

      acceptorControl.stop();

      Assert.assertFalse(acceptorControl.isStarted());

      try
      {
         sf.createSession(false, true, true);
         Assert.fail("acceptor must not accept connections when stopped accepting");
      }
      catch (Exception e)
      {
      }

      acceptorControl.start();

      Assert.assertTrue(acceptorControl.isStarted());

      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf = locator.createSessionFactory();
      session = sf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();

      acceptorControl.stop();

      Assert.assertFalse(acceptorControl.isStarted());

      try
      {
         sf.createSession(false, true, true);
         Assert.fail("acceptor must not accept connections when stopped accepting");
      }
      catch (Exception e)
      {
      }

   }

   public void testNotifications() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         RandomUtil.randomString());
      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();

      service.getManagementService().addNotificationListener(notifListener);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      acceptorControl.stop();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.ACCEPTOR_STOPPED, notif.getType());
      Assert.assertEquals(InVMAcceptorFactory.class.getName(),
                          notif.getProperties().getSimpleStringProperty(new SimpleString("factory")).toString());

      acceptorControl.start();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.ACCEPTOR_STARTED, notif.getType());
      Assert.assertEquals(InVMAcceptorFactory.class.getName(),
                          notif.getProperties().getSimpleStringProperty(new SimpleString("factory")).toString());
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

   protected AcceptorControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createAcceptorControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
