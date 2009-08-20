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

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyAcceptorFactory;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSServerControl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;

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

   private static final long CONNECTION_TTL = 1000;

   private static final long PING_PERIOD = CONNECTION_TTL / 2;

   // Attributes ----------------------------------------------------

   private HornetQServer server;
   
   JMSServerManagerImpl serverManager;
   
   private InVMContext context;

   // Static --------------------------------------------------------

   private void startHornetQServer(String acceptorFactory) throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactory));
      server = HornetQ.newHornetQServer(conf, mbeanServer, false);
      server.start();

      context = new InVMContext();
      serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();
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

   protected JMSServerControl createManagementControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }
   
   protected void tearDown() throws Exception
   {
      serverManager = null;
      
      server = null;
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void doListConnectionIDs(String acceptorFactory, String connectorFactory) throws Exception
   {
      try
      {
         startHornetQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         assertEquals(0, control.listConnectionIDs().length);

         ConnectionFactory cf1 = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection = cf1.createConnection();

         String[] connectionIDs = control.listConnectionIDs();
         assertEquals(1, connectionIDs.length);
       
         ConnectionFactory cf2 = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection2 = cf2.createConnection();
         assertEquals(2, control.listConnectionIDs().length);

         connection.close();
         Thread.sleep(2 * CONNECTION_TTL);

         connectionIDs = control.listConnectionIDs();
         assertEquals("got " + Arrays.asList(connectionIDs), 1, connectionIDs.length);

         assertEquals(1, control.listConnectionIDs().length);

         connection2.close();
         Thread.sleep(2 * CONNECTION_TTL);

         assertEquals(0, control.listConnectionIDs().length);
      }
      finally
      {
         if (serverManager != null)
         {
            serverManager.stop();
         }
         
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doListSessions(String acceptorFactory, String connectorFactory) throws Exception
   {
      try
      {
         startHornetQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         assertEquals(0, control.listConnectionIDs().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection = cf.createConnection();

         String[] connectionIDs = control.listConnectionIDs();
         assertEquals(1, connectionIDs.length);
         String connectionID = connectionIDs[0];

         String[] sessions = control.listSessions(connectionID);
         assertEquals(1, sessions.length);
         connection.close();
         sessions = control.listSessions(connectionID);
         assertEquals("got " + Arrays.asList(sessions), 0, sessions.length);

         connection.close();

         Thread.sleep(2 * CONNECTION_TTL);

         assertEquals(0, control.listConnectionIDs().length);
      }
      finally
      {
         if (serverManager != null)
         {
            serverManager.stop();
         }
         
         
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doListClientConnections(String acceptorFactory, String connectorFactory) throws Exception
   {
      try
      {
         startHornetQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection = cf.createConnection();

         String[] remoteAddresses = control.listRemoteAddresses();
         assertEquals(1, remoteAddresses.length);

         for (String remoteAddress : remoteAddresses)
         {
            System.out.println(remoteAddress);
         }
         
         connection.close();

         Thread.sleep(2 * CONNECTION_TTL);

         remoteAddresses = control.listRemoteAddresses();         
         assertEquals("got " + Arrays.asList(remoteAddresses), 0, remoteAddresses.length);
      }
      finally
      {
         if (serverManager != null)
         {
            serverManager.stop();
         }
         
         
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForAddress(String acceptorFactory, String connectorFactory) throws Exception
   {
      try
      {
         startHornetQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         assertEquals(0, server.getConnectionCount());
         assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection = cf.createConnection();

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

         boolean gotException = exceptionLatch.await(2 * CONNECTION_TTL, TimeUnit.MILLISECONDS);
         assertTrue("did not received the expected JMSException", gotException);

         remoteAddresses = control.listRemoteAddresses();         
         assertEquals("got " + Arrays.asList(remoteAddresses), 0, remoteAddresses.length);
         assertEquals(0, server.getConnectionCount());
         
         connection.close();
      }
      finally
      {
         if (serverManager != null)
         {
            serverManager.stop();
         }
         
         if (server != null)
         {
            server.stop();
         }
      }
   }

   private void doCloseConnectionsForUnknownAddress(String acceptorFactory, String connectorFactory) throws Exception
   {
      String unknownAddress = randomString();

      try
      {
         startHornetQServer(acceptorFactory);

         JMSServerControl control = createManagementControl();

         assertEquals(0, server.getConnectionCount());
         assertEquals(0, control.listRemoteAddresses().length);

         ConnectionFactory cf = JMSUtil.createFactory(connectorFactory, CONNECTION_TTL, PING_PERIOD);
         Connection connection = cf.createConnection();

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

         boolean gotException = exceptionLatch.await(2 * CONNECTION_TTL, TimeUnit.MILLISECONDS);
         assertFalse(gotException);

         assertEquals(1, control.listRemoteAddresses().length);
         assertEquals(1, server.getConnectionCount());
         
         connection.close();

      }
      finally
      {
         if (serverManager != null)
         {
            serverManager.stop();
         }
         
         if (server != null)
         {
            server.stop();
         }
      }
   }

   // Inner classes -------------------------------------------------

}