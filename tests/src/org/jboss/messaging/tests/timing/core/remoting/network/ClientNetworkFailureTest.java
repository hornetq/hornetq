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

package org.jboss.messaging.tests.timing.core.remoting.network;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Acceptor;
import org.jboss.messaging.core.remoting.TransportType;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptor;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import static org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport.PING_INTERVAL;
import static org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport.PING_TIMEOUT;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class ClientNetworkFailureTest extends TestCase
{

   // Constants -----------------------------------------------------
   Logger log = Logger.getLogger(ClientNetworkFailureTest.class);
   private MessagingService messagingService;
   private RemotingServiceImpl minaService;
   private NetworkFailureFilter networkFailureFilter;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientNetworkFailureTest(String name)
   {
      super(name);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      ConfigurationImpl newConfig = new ConfigurationImpl();
      newConfig.getConnectionParams().setInVMOptimisationEnabled(false);
      newConfig.setHost("localhost");
      newConfig.setPort(5400);
      newConfig.setTransport(TransportType.TCP);
      newConfig.getConnectionParams().setPingInterval(PING_INTERVAL);
      newConfig.getConnectionParams().setPingTimeout(PING_TIMEOUT);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(newConfig);
      messagingService.start();
      minaService = (RemotingServiceImpl) messagingService.getServer().getRemotingService();
      networkFailureFilter = new NetworkFailureFilter();
      List<Acceptor> acceptor = minaService.getAcceptors();
      MinaAcceptor minaAcceptor = (MinaAcceptor) acceptor.get(0);
      minaAcceptor.getFilterChain().addFirst("network-failure",
              networkFailureFilter);

      assertActiveConnectionsOnTheServer(0);
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertActiveConnectionsOnTheServer(0);
      messagingService.stop();

      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testServerResourcesCleanUpWhenClientCommThrowsException()
           throws Exception
   {
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(new LocationImpl(TCP, "localhost", 5400));

      ClientConnection conn = cf.createConnection();

      assertActiveConnectionsOnTheServer(1);

      final CountDownLatch exceptionLatch = new CountDownLatch(2);
      conn.setRemotingSessionListener(new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            exceptionLatch.countDown();
         }
      });
      RemotingSessionListener listener = new FailureListenerWithLatch(exceptionLatch);
      minaService.addRemotingSessionListener(listener);

      networkFailureFilter.messageSentThrowsException = new IOException(
              "Client is unreachable");
      networkFailureFilter.messageReceivedDropsPacket = true;

      boolean gotExceptionsOnTheServerAndTheClient = exceptionLatch.await(
              PING_INTERVAL + PING_TIMEOUT + 5000, MILLISECONDS);
      assertTrue(gotExceptionsOnTheServerAndTheClient);
      //now we  need to wait for the server to detect the client failover
      //Thread.sleep((PING_INTERVAL + PING_TIMEOUT) * 1000);
      assertActiveConnectionsOnTheServer(0);

      assertTrue(conn.isClosed());

      minaService.removeRemotingSessionListener(listener);
   }

   public void testServerResourcesCleanUpWhenClientCommDropsPacket()
           throws Exception
   {
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(new LocationImpl(TCP, "localhost", 5400));

      ClientConnection conn = cf.createConnection();

      final CountDownLatch exceptionLatch = new CountDownLatch(1);

      RemotingSessionListener listener = new FailureListenerWithLatch(exceptionLatch);
      minaService.addRemotingSessionListener(listener);

      assertActiveConnectionsOnTheServer(1);

      networkFailureFilter.messageSentDropsPacket = true;
      networkFailureFilter.messageReceivedDropsPacket = true;

      boolean gotExceptionOnTheServer = exceptionLatch.await(
              PING_INTERVAL + PING_TIMEOUT + 5000, MILLISECONDS);
      assertTrue(gotExceptionOnTheServer);
      //now we  need to wait for the server to detect the client failover
      //Thread.sleep((PING_INTERVAL + PING_TIMEOUT) * 1000);
      assertActiveConnectionsOnTheServer(0);

      try
      {
         conn.close();
         fail("close should fail since client resources must have been cleaned up on the server side");
      }
      catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private final class FailureListenerWithLatch implements RemotingSessionListener
   {
      private final CountDownLatch exceptionLatch;

      private FailureListenerWithLatch(CountDownLatch exceptionLatch)
      {
         this.exceptionLatch = exceptionLatch;
      }

      public void sessionDestroyed(long sessionID, MessagingException me)
      {
         log.warn("got expected exception on the server");
         exceptionLatch.countDown();
      }
   }

   private void assertActiveConnectionsOnTheServer(int expectedSize)
           throws Exception
   {
      assertEquals(expectedSize, messagingService.getServer().getServerManagement().getConnectionCount());
   }
}
