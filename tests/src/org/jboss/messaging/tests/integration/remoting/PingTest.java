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

import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.client.impl.ConnectionManagerImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.server.impl.RemotingServiceImpl;
import org.jboss.messaging.core.server.MessagingServer;
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

   private static final long CLIENT_FAILURE_CHECK_PERIOD = 500;

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      server = createServer(false, config);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();
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

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig);

      csf.setClientFailureCheckPeriod(CLIENT_FAILURE_CHECK_PERIOD);
      csf.setConnectionTTL(CLIENT_FAILURE_CHECK_PERIOD * 2);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(CLIENT_FAILURE_CHECK_PERIOD * 10);

      assertNull(clientListener.getException());

      assertNull(serverListener.getException());

      RemotingConnection serverConn2 = server.getRemotingService().getConnections().iterator().next();

      assertTrue(serverConn == serverConn2);

      session.close();
   }

   /*
    * Test that no failure listeners are triggered in a non failure case with no pinging going on
    */
   public void testNoFailureNoPinging() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig);
      csf.setClientFailureCheckPeriod(-1);
      csf.setConnectionTTL(-1);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);

      assertNull(clientListener.getException());

      assertNull(serverListener.getException());

      RemotingConnection serverConn2 = server.getRemotingService().getConnections().iterator().next();

      assertTrue(serverConn == serverConn2);

      session.close();
   }

   /*
    * Test the server timing out a connection since it doesn't receive a ping in time
    */
   public void testServerFailureNoPing() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactoryImpl csf = new ClientSessionFactoryImpl(transportConfig);

      csf.setClientFailureCheckPeriod(CLIENT_FAILURE_CHECK_PERIOD);
      csf.setConnectionTTL(CLIENT_FAILURE_CHECK_PERIOD * 2);

      Listener clientListener = new Listener();

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      session.addFailureListener(clientListener);

      RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionInternal)session).getConnection();

      // We need to get it to stop pinging

      ((ConnectionManagerImpl)csf.getConnectionManagers()[0]).cancelPingerForConnectionID(conn.getID());

      RemotingConnection serverConn = null;

      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      for (int i = 0; i < 1000; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (server.getRemotingService().getConnections().isEmpty() && clientListener.getException() != null)
         {
            break;
         }

         Thread.sleep(10);
      }

      assertTrue(server.getRemotingService().getConnections().isEmpty());

      // The client listener should be called too since the server will close it from the server side which will result
      // in the
      // netty detecting closure on the client side and then calling failure listener
      assertNotNull(clientListener.getException());

      assertNotNull(serverListener.getException());

      session.close();
   }

   /*
   * Test the client triggering failure due to no ping from server received in time
   */
   public void testClientFailureNoServerPing() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      ClientSessionFactory csf = new ClientSessionFactoryImpl(transportConfig);

      csf.setClientFailureCheckPeriod(CLIENT_FAILURE_CHECK_PERIOD);
      csf.setConnectionTTL(CLIENT_FAILURE_CHECK_PERIOD * 2);

      ClientSession session = csf.createSession(false, true, true);

      assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      ((RemotingServiceImpl)server.getRemotingService()).cancelPingerForConnectionID(serverConn.getID());

      for (int i = 0; i < 1000; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (server.getRemotingService().getConnections().isEmpty() && clientListener.getException() != null)
         {
            break;
         }

         Thread.sleep(10);
      }
            
      assertNotNull(clientListener.getException());
      //Server connection will be closed too, when client closes client side connection after failure is detected
      assertTrue(server.getRemotingService().getConnections().isEmpty());

      session.close();

      tearDown();

      setUp();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}