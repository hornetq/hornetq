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

package org.hornetq.tests.integration.jms.server.management;

import java.util.ArrayList;
import java.util.List;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.ConnectionFactoryControl;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSNotificationType;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;

/**
 * A Connection Factory Control Test
 *
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 *
 * Created 13 nov. 2008 16:50:53
 *
 *
 */
public class ConnectionFactoryControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   private InVMContext ctx;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCreateCF() throws Exception
   {
      JMSServerControl control = createJMSControl();
      control.createConnectionFactory("test", false, false, 0, "invm", "test");

      ConnectionFactoryControl controlCF = createCFControl("test");

      HornetQConnectionFactory cf = (HornetQConnectionFactory)ctx.lookup("test");

      assertFalse(cf.isCompressLargeMessage());

      controlCF.setCompressLargeMessages(true);

      cf = (HornetQConnectionFactory)ctx.lookup("test");
      assertTrue(cf.isCompressLargeMessage());

      stopServer();

      Thread.sleep(500);

      startServer();

      cf = (HornetQConnectionFactory)ctx.lookup("test");
      assertTrue(cf.isCompressLargeMessage());

   }

   //make sure notifications are always received no matter whether 
   //a CF is created via JMSServerControl or by JMSServerManager directly. 
   public void testCreateCFNotification() throws Exception
   {
      JMSUtil.JMXListener listener = new JMSUtil.JMXListener();
      this.mbeanServer.addNotificationListener(ObjectNameBuilder.DEFAULT.getJMSServerObjectName(), listener, null, null);

      List<String> connectors = new ArrayList<String>();
      connectors.add("invm");
      
      this.serverManager.createConnectionFactory("NewCF",
                                                  false,
                                                  JMSFactoryType.CF,
                                                  connectors,
                                                  "/NewConnectionFactory");

      Notification notif = listener.getNotification();
      
      assertEquals(JMSNotificationType.CONNECTION_FACTORY_CREATED.toString(), notif.getType());
      assertEquals("NewCF", notif.getMessage());

      this.serverManager.destroyConnectionFactory("NewCF");
      
      notif = listener.getNotification();
      assertEquals(JMSNotificationType.CONNECTION_FACTORY_DESTROYED.toString(), notif.getType());
      assertEquals("NewCF", notif.getMessage());

      JMSServerControl control = createJMSControl();
      
      control.createConnectionFactory("test", false, false, 0, "invm", "test");
      
      notif = listener.getNotification();
      assertEquals(JMSNotificationType.CONNECTION_FACTORY_CREATED.toString(), notif.getType());
      assertEquals("test", notif.getMessage());

      control.destroyConnectionFactory("test");
      
      notif = listener.getNotification();
      assertEquals(JMSNotificationType.CONNECTION_FACTORY_DESTROYED.toString(), notif.getType());
      assertEquals("test", notif.getMessage());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      startServer();

   }

   /**
    * @throws Exception
    */
   protected void startServer() throws Exception
   {
      Configuration conf = createDefaultConfig(false);
      conf.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setSharedStore(false);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, true);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.start();

      ctx = new InVMContext();

      serverManager.setContext(ctx);
      serverManager.activated();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServer();

      super.tearDown();
   }

   /**
    * @throws Exception
    */
   protected void stopServer() throws Exception
   {
      serverManager.stop();

      server.stop();

      serverManager = null;

      server = null;
   }

   protected ConnectionFactoryControl createCFControl(String name) throws Exception
   {
      return ManagementControlHelper.createConnectionFactoryControl(name, mbeanServer);
   }

   protected JMSServerControl createJMSControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
