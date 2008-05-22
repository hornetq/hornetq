/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.tests.unit.jms.network;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport.KEEP_ALIVE_INTERVAL;
import static org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport.KEEP_ALIVE_TIMEOUT;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.TransportType;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.logging.Logger;
import junit.framework.TestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ClientNetworkFailureTest extends TestCase
{

   // Constants -----------------------------------------------------
   Logger log = Logger.getLogger(ClientNetworkFailureTest.class);
   private MessagingServer server;
   private MinaService minaService;
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
      newConfig.setInvmDisabled(true);
      newConfig.setHost("localhost");
      newConfig.setPort(5400);
      newConfig.setTransport(TransportType.TCP);
      newConfig.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
      newConfig.setKeepAliveTimeout(KEEP_ALIVE_TIMEOUT);
      server = new MessagingServerImpl(newConfig);
      server.start();
      minaService = (MinaService) server.getRemotingService();
      networkFailureFilter = new NetworkFailureFilter();
      minaService.getFilterChain().addFirst("network-failure",
            networkFailureFilter);

      assertActiveConnectionsOnTheServer(0);
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertActiveConnectionsOnTheServer(0);
      server.stop();
      //minaService.start();

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
            KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(gotExceptionsOnTheServerAndTheClient);
      //now we  need to wait for the server to detect the client failover
      Thread.sleep((KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT) * 1000);
      assertActiveConnectionsOnTheServer(0);

      try
      {
         conn.close();
         fail("close should fail since client resources must have been cleaned up on the server side");
      } catch (Exception e)
      {
      }

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
            KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT + 5, SECONDS);
      assertTrue(gotExceptionOnTheServer);
      //now we  need to wait for the server to detect the client failover
      Thread.sleep((KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT) * 1000);
      assertActiveConnectionsOnTheServer(0);

      try
      {
         conn.close();
         fail("close should fail since client resources must have been cleaned up on the server side");
      } catch (Exception e)
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
      ConnectionManager cm = server
      .getConnectionManager();
      assertEquals(expectedSize, cm.getActiveConnections().size());
   }
}
