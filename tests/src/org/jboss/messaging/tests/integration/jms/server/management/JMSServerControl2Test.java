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

package org.jboss.messaging.tests.integration.jms.server.management;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.tests.integration.management.ManagementControlHelper;
import org.jboss.messaging.tests.integration.management.ManagementTestBase;
import org.jboss.messaging.tests.unit.util.InVMContext;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A QueueControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSServerControl2Test extends ManagementTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSServerControl2Test.class);

   // Attributes ----------------------------------------------------

   private InVMContext context;

   // Static --------------------------------------------------------

   private MessagingServer startMessagingServer(String acceptorFactory) throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactory));
      MessagingServer server = Messaging.newMessagingServer(conf, mbeanServer, false);
      server.start();

      context = new InVMContext();
      JMSServerManagerImpl serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      return server;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testListClientConnectionsForInVM() throws Exception
   {
      doListClientConnections(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   public void testListClientConnectionsForNetty() throws Exception
   {
      doListClientConnections(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   public void testCloseConnectionsForAddressForInVM() throws Exception
   {
      doCloseConnectionsForAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   public void testCloseConnectionsForAddressForNetty() throws Exception
   {
      doCloseConnectionsForAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   public void testCloseConnectionsForUnknownAddressForInVM() throws Exception
   {
      doCloseConnectionsForUnknownAddress(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   public void testCloseConnectionsForUnknownAddressForNetty() throws Exception
   {
      doCloseConnectionsForUnknownAddress(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   public void testListSessionsForInVM() throws Exception
   {
      doListSessions(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   public void testListSessionsForNetty() throws Exception
   {
      doListSessions(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   public void testListConnectionIDsForInVM() throws Exception
   {
      doListConnectionIDs(InVMAcceptorFactory.class.getName(), InVMConnectorFactory.class.getName());
   }

   public void testListConnectionIDsForNetty() throws Exception
   {
      doListConnectionIDs(NettyAcceptorFactory.class.getName(), NettyConnectorFactory.class.getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected JMSServerControlMBean createManagementControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private void doListConnectionIDs(String acceptorFactory, String connectorFactory) throws Exception
   {
      MessagingServer server = null;
      try
      {
         server = startMessagingServer(acceptorFactory);

         JMSServerControlMBean control = createManagementControl();

         assertEquals(0, control.listConnectionIDs().length);

         Connection connection = JMSUtil.createConnection(connectorFactory);

         String[] connectionIDs = control.listConnectionIDs();
         assertEquals(1, connectionIDs.length);

         Connection connection2 = JMSUtil.createConnection(connectorFactory);
         assertEquals(2, control.listConnectionIDs().length);

         connection.close();
         Thread.sleep(500);

         assertEquals(1, control.listConnectionIDs().length);

         connection2.close();
         Thread.sleep(500);

         assertEquals(0, control.listConnectionIDs().length);
      }
      finally
      {
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doListSessions(String acceptorFactory, String connectorFactory) throws Exception
   {
      MessagingServer server = null;
      try
      {
         server = startMessagingServer(acceptorFactory);

         JMSServerControlMBean control = createManagementControl();

         assertEquals(0, control.listConnectionIDs().length);

         Connection connection = JMSUtil.createConnection(connectorFactory);

         String[] connectionIDs = control.listConnectionIDs();
         assertEquals(1, connectionIDs.length);
         String connectionID = connectionIDs[0];

         String[] sessions = control.listSessions(connectionID);
         assertEquals(1, sessions.length);
         connection.close();
         sessions = control.listSessions(connectionID);
         assertEquals(0, sessions.length);

         connection.close();

         Thread.sleep(500);

         assertEquals(0, control.listConnectionIDs().length);
      }
      finally
      {
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doListClientConnections(String acceptorFactory, String connectorFactory) throws Exception
   {
      MessagingServer server = null;
      try
      {
         server = startMessagingServer(acceptorFactory);

         JMSServerControlMBean control = createManagementControl();

         assertEquals(0, control.listRemoteAddresses().length);

         Connection connection = JMSUtil.createConnection(connectorFactory);

         String[] remoteAddresses = control.listRemoteAddresses();
         assertEquals(1, remoteAddresses.length);

         for (String remoteAddress : remoteAddresses)
         {
            System.out.println(remoteAddress);
         }
         
         log.info("*** closing connection");
         
         connection.close();

         // FIXME: with Netty, the server is not notified immediately that the connection is closed
         Thread.sleep(1000);

         assertEquals(0, control.listRemoteAddresses().length);
      }
      finally
      {
         if (server != null)
         {
            server.stop();
         }
      }

   }

   private void doCloseConnectionsForAddress(String acceptorFactory, String connectorFactory) throws Exception
   {
      MessagingServer server = null;
      try
      {
         server = startMessagingServer(acceptorFactory);

         JMSServerControlMBean control = createManagementControl();

         assertEquals(0, server.getConnectionCount());
         assertEquals(0, control.listRemoteAddresses().length);

         Connection connection = JMSUtil.createConnection(connectorFactory);

         assertEquals(1, server.getConnectionCount());

         String[] remoteAddresses = control.listRemoteAddresses();
         assertEquals(1, remoteAddresses.length);
         String remoteAddress = remoteAddresses[0];

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener()
         {
            public void onException(JMSException e)
            {
               exceptionLatch.countDown();
            }
         });

         assertTrue(control.closeConnectionsForAddress(remoteAddress));

         boolean gotException = exceptionLatch.await(5, TimeUnit.SECONDS);
         assertTrue("did not received the expected JMSException", gotException);
         assertEquals(0, control.listRemoteAddresses().length);
         assertEquals(0, server.getConnectionCount());
      }
      finally
      {
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForUnknownAddress(String acceptorFactory, String connectorFactory) throws Exception
   {
      String unknownAddress = randomString();

      MessagingServer server = null;

      try
      {
         server = startMessagingServer(acceptorFactory);

         JMSServerControlMBean control = createManagementControl();

         assertEquals(0, server.getConnectionCount());
         assertEquals(0, control.listRemoteAddresses().length);

         Connection connection = JMSUtil.createConnection(connectorFactory);

         assertEquals(1, server.getConnectionCount());
         String[] remoteAddresses = control.listRemoteAddresses();
         assertEquals(1, remoteAddresses.length);

         final CountDownLatch exceptionLatch = new CountDownLatch(1);
         connection.setExceptionListener(new ExceptionListener()
         {
            public void onException(JMSException e)
            {
               exceptionLatch.countDown();
            }
         });

         assertFalse(control.closeConnectionsForAddress(unknownAddress));

         boolean gotException = exceptionLatch.await(500, TimeUnit.MILLISECONDS);
         assertFalse(gotException);

         assertEquals(1, control.listRemoteAddresses().length);
         assertEquals(1, server.getConnectionCount());

      }
      finally
      {
         if (server != null)
         {
            server.stop();
         }
      }
   }

   // Inner classes -------------------------------------------------

}