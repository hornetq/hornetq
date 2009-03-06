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

package org.jboss.messaging.tests.integration.remoting;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class PingTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PingTest.class);

   private static final long PING_INTERVAL = 500;

   // Attributes ----------------------------------------------------

   private MessagingService messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      messagingService = createService(false, config);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();
      super.tearDown();
   }

   class Listener implements FailureListener
   {
      volatile MessagingException me;

      public boolean connectionFailed(MessagingException me)
      {
         this.me = me;

         return true;
      }

      public MessagingException getException()
      {
         return me;
      }
   };

   /*
    * Test that no failure listeners are triggered in a non failure case with pinging going on
    */
   public void testNoFailureWithPinging() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig,
                                                              null,
                                                              DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                              PING_INTERVAL,
                                                              DEFAULT_CONNECTION_TTL,
                                                              DEFAULT_CALL_TIMEOUT,
                                                              DEFAULT_CONSUMER_WINDOW_SIZE,
                                                              DEFAULT_CONSUMER_MAX_RATE,
                                                              DEFAULT_SEND_WINDOW_SIZE,
                                                              DEFAULT_PRODUCER_MAX_RATE,
                                                              DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                              DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                              DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                              DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                              DEFAULT_AUTO_GROUP,
                                                              DEFAULT_MAX_CONNECTIONS,
                                                              DEFAULT_PRE_ACKNOWLEDGE,
                                                              DEFAULT_ACK_BATCH_SIZE,
                                                              DEFAULT_RETRY_INTERVAL,
                                                              DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                              DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                              DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(PING_INTERVAL * 3);

      assertNull(clientListener.getException());

      assertNull(serverListener.getException());

      RemotingConnection serverConn2 = messagingService.getServer()
                                                       .getRemotingService()
                                                       .getConnections()
                                                       .iterator()
                                                       .next();

      assertTrue(serverConn == serverConn2);

      session.close();
   }

   /*
    * Test that no failure listeners are triggered in a non failure case with no pinging going on
    */
   public void testNoFailureNoPinging() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig,
                                                              null,
                                                              DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                              PING_INTERVAL,
                                                              DEFAULT_CONNECTION_TTL,
                                                              DEFAULT_CALL_TIMEOUT,
                                                              DEFAULT_CONSUMER_WINDOW_SIZE,
                                                              DEFAULT_CONSUMER_MAX_RATE,
                                                              DEFAULT_SEND_WINDOW_SIZE,
                                                              DEFAULT_PRODUCER_MAX_RATE,
                                                              DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                              DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                              DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                              DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                              DEFAULT_AUTO_GROUP,
                                                              DEFAULT_MAX_CONNECTIONS,
                                                              DEFAULT_PRE_ACKNOWLEDGE,
                                                              DEFAULT_ACK_BATCH_SIZE,
                                                              DEFAULT_RETRY_INTERVAL,
                                                              DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                              DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                              DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(PING_INTERVAL * 3);

      assertNull(clientListener.getException());

      assertNull(serverListener.getException());

      RemotingConnection serverConn2 = messagingService.getServer()
                                                       .getRemotingService()
                                                       .getConnections()
                                                       .iterator()
                                                       .next();

      assertTrue(serverConn == serverConn2);

      session.close();
   }

   /*
    * Test the server timing out a connection since it doesn't receive a ping in time
    */
   public void testServerFailureNoPing() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig,
                                                              null,
                                                              DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                              PING_INTERVAL,
                                                              (long)(PING_INTERVAL * 1.5),
                                                              DEFAULT_CALL_TIMEOUT,
                                                              DEFAULT_CONSUMER_WINDOW_SIZE,
                                                              DEFAULT_CONSUMER_MAX_RATE,
                                                              DEFAULT_SEND_WINDOW_SIZE,
                                                              DEFAULT_PRODUCER_MAX_RATE,
                                                              DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                              DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                              DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                              DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                              DEFAULT_AUTO_GROUP,
                                                              DEFAULT_MAX_CONNECTIONS,
                                                              DEFAULT_PRE_ACKNOWLEDGE,
                                                              DEFAULT_ACK_BATCH_SIZE,
                                                              DEFAULT_RETRY_INTERVAL,
                                                              DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                              DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                              DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      Listener clientListener = new Listener();

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      session.addFailureListener(clientListener);

      RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionInternal)session).getConnection();

      // We need to get it to send one ping then stop
      conn.stopPingingAfterOne();

      RemotingConnection serverConn = null;

      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      for (int i = 0; i < 40; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (messagingService.getServer().getRemotingService().getConnections().isEmpty() && clientListener.getException() != null)
         {
            break;
         }

         Thread.sleep(PING_INTERVAL);
      }

      // The client listener should be called too since the server will close it from the server side which will result
      // in the
      // netty detecting closure on the client side and then calling failure listener
      assertNotNull(clientListener.getException());

      assertNotNull(serverListener.getException());

      assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());

      session.close();
   }

   /*
   * Test the client triggering failure due to no pong received in time
   */
   public void testClientFailureNoPong() throws Exception
   {
      Interceptor noPongInterceptor = new Interceptor()
      {
         public boolean intercept(Packet packet, RemotingConnection conn) throws MessagingException
         {
            log.info("In interceptor, packet is " + packet.getType());
            if (packet.getType() == PacketImpl.PING)
            {
               log.info("Ignoring Ping packet.. it will be dropped");
               return false;
            }
            else
            {
               return true;
            }
         }
      };

      messagingService.getServer().getRemotingService().addInterceptor(noPongInterceptor);

      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig,
                                                              null,
                                                              DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                              PING_INTERVAL,
                                                              (long)(PING_INTERVAL * 1.5),
                                                              DEFAULT_CALL_TIMEOUT,
                                                              DEFAULT_CONSUMER_WINDOW_SIZE,
                                                              DEFAULT_CONSUMER_MAX_RATE,
                                                              DEFAULT_SEND_WINDOW_SIZE,
                                                              DEFAULT_PRODUCER_MAX_RATE,
                                                              DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                              DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                              DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                              DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                              DEFAULT_AUTO_GROUP,
                                                              DEFAULT_MAX_CONNECTIONS,
                                                              DEFAULT_PRE_ACKNOWLEDGE,
                                                              DEFAULT_ACK_BATCH_SIZE,
                                                              DEFAULT_RETRY_INTERVAL,
                                                              DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                              0,
                                                              0);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = messagingService.getServer().getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = messagingService.getServer().getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      for (int i = 0; i < 40; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (messagingService.getServer().getRemotingService().getConnections().isEmpty() && clientListener.getException() != null)
         {
            break;
         }

         Thread.sleep(PING_INTERVAL);
      }

      assertNotNull(clientListener.getException());

      // We don't receive an exception on the server in this case
      assertNull(serverListener.getException());

      assertTrue(messagingService.getServer().getRemotingService().getConnections().isEmpty());

      messagingService.getServer().getRemotingService().removeInterceptor(noPongInterceptor);

      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}