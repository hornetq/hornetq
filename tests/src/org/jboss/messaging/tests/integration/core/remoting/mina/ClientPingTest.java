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

package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class ClientPingTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", TestSupport.PORT);
      config.getConnectionParams().setPingInterval(TestSupport.PING_INTERVAL);
      config.getConnectionParams().setPingTimeout(TestSupport.PING_TIMEOUT);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();
   }

   public void testKeepAliveWithClientOK() throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(1);

      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            latch.countDown();
         }
      };
      messagingService.getServer().getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(TestSupport.PING_INTERVAL);
      connectionParams.setPingTimeout(TestSupport.PING_TIMEOUT);
      MinaConnector connector = new MinaConnector(new LocationImpl(TCP, "localhost", TestSupport.PORT), connectionParams, new PacketDispatcherImpl(null));
      connector.connect();

      boolean firedKeepAliveNotification = latch.await(TestSupport.PING_INTERVAL
              + TestSupport.PING_TIMEOUT + 2000, MILLISECONDS);
      assertFalse(firedKeepAliveNotification);

      messagingService.getServer().getRemotingService().removeRemotingSessionListener(listener);
      //connector.disconnect();

      // verify(factory);
   }

   public void testKeepAliveWithClientNotResponding() throws Throwable
   {

      final long[] clientSessionIDNotResponding = new long[1];
      final CountDownLatch latch = new CountDownLatch(1);

      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            clientSessionIDNotResponding[0] = sessionID;
            latch.countDown();
         }
      };
      messagingService.getServer().getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(TestSupport.PING_INTERVAL);
      connectionParams.setPingTimeout(TestSupport.PING_TIMEOUT);

      LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
      MinaConnector connector = new MinaConnector(location, connectionParams, new PacketDispatcherImpl(null));

      RemotingSession session = connector.connect();
      connector.getDispatcher().register(new NotRespondingPacketHandler());
      long clientSessionID = session.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.PING_INTERVAL
              + TestSupport.PING_TIMEOUT + 2000, MILLISECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);
      assertNotNull(clientSessionIDNotResponding[0]);
      //assertEquals(clientSessionID, clientSessionIDNotResponding[0]);

      messagingService.getServer().getRemotingService().removeRemotingSessionListener(listener);
      connector.disconnect();
   }

   public void testKeepAliveWithClientTooLongToRespond() throws Throwable
   {
      PacketHandler tooLongRespondHandler = new PacketHandler()
      {
         public long getID()
         {
            return 0;
         }

         public void handle(Packet packet, PacketReturner sender)
         {
            try
            {
               synchronized (this)
               {
                  wait(2 * 3600);
               }
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            try
            {
               sender.send(new Pong(randomLong(), false));
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };
      try
      {
         ConnectionParams connectionParams = new ConnectionParamsImpl();
         connectionParams.setPingInterval(TestSupport.PING_INTERVAL);
         connectionParams.setPingTimeout(TestSupport.PING_TIMEOUT);
         LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
         MinaConnector connector = new MinaConnector(location, connectionParams,
                 new PacketDispatcherImpl(null));

         RemotingSession session = connector.connect();
         connector.getDispatcher().register(tooLongRespondHandler);
         long clientSessionID = session.getID();

         final AtomicLong clientSessionIDNotResponding = new AtomicLong(-1);
         final CountDownLatch latch = new CountDownLatch(1);

         RemotingSessionListener listener = new RemotingSessionListener()
         {
            public void sessionDestroyed(long sessionID, MessagingException me)
            {
               clientSessionIDNotResponding.set(sessionID);
               latch.countDown();
            }
         };
         messagingService.getServer().getRemotingService().addRemotingSessionListener(listener);

         boolean firedKeepAliveNotification = latch.await(TestSupport.PING_INTERVAL
                 + TestSupport.PING_TIMEOUT + 2000, MILLISECONDS);
         assertTrue("notification has not been received", firedKeepAliveNotification);
         //assertEquals(clientSessionID, clientSessionIDNotResponding.longValue());

         messagingService.getServer().getRemotingService().removeRemotingSessionListener(listener);
         connector.disconnect();

      }
      finally
      {
         // test is done: wake up the factory
         synchronized (tooLongRespondHandler)
         {
            tooLongRespondHandler.notify();
         }
      }
   }

   public void testKeepAliveWithClientRespondingAndClientNotResponding()
           throws Throwable
   {
      final AtomicLong sessionIDNotResponding = new AtomicLong(-1);
      final CountDownLatch latch = new CountDownLatch(1);

      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            sessionIDNotResponding.set(sessionID);
            latch.countDown();
         }
      };
      //assign this after we have connected to replace the pong handler
      PacketHandler notRespondingPacketHandler = new NotRespondingPacketHandler();
      messagingService.getServer().getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setPingInterval(TestSupport.PING_INTERVAL);
      connectionParams.setPingTimeout(TestSupport.PING_TIMEOUT);
      LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
      MinaConnector connectorNotResponding = new MinaConnector(location, new PacketDispatcherImpl(null));
      MinaConnector connectorResponding = new MinaConnector(location, new PacketDispatcherImpl(null));

      RemotingSession sessionNotResponding = connectorNotResponding.connect();
      connectorNotResponding.getDispatcher().register(notRespondingPacketHandler);
      long clientSessionIDNotResponding = sessionNotResponding.getID();


      RemotingSession sessionResponding = connectorResponding.connect();
      long clientSessionIDResponding = sessionResponding.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.PING_INTERVAL
              + TestSupport.PING_TIMEOUT + 2000, MILLISECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);

      //assertEquals(clientSessionIDNotResponding, sessionIDNotResponding.longValue());
      assertNotSame(clientSessionIDResponding, sessionIDNotResponding.longValue());

      messagingService.getServer().getRemotingService().removeRemotingSessionListener(listener);
      connectorNotResponding.disconnect();
      connectorResponding.disconnect();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


   private static class NotRespondingPacketHandler implements PacketHandler
   {
      public long getID()
            {
               return 0;
            }

      public void handle(Packet packet, PacketReturner sender)
            {
               //dont do anything. i.e no response ping
            }
   }
}