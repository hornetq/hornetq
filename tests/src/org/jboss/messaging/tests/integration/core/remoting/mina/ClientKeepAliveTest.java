/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.*;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.ClientKeepAliveHandler;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.server.impl.MessagingServerPacketHandler;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class ClientKeepAliveTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer messagingServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", TestSupport.PORT);
      config.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      config.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      messagingServer = new MessagingServerImpl(config);
      messagingServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingServer.stop();
   }

   public void testKeepAliveWithClientOK() throws Exception
   {
      KeepAliveHandler factory = createMock(KeepAliveHandler.class);

      // client never send ping
      //expect(factory.isAlive(isA(Ping.class), isA(Pong.class))).andStubReturn(true);
      //expect(factory.isAlive(isA(Ping.class), isA(Pong.class))).andStubReturn(false);

      replay(factory);

      final CountDownLatch latch = new CountDownLatch(1);

      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            latch.countDown();
         }
      };
      messagingServer.getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      connectionParams.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      MinaConnector connector = new MinaConnector(new LocationImpl(TCP, "localhost", TestSupport.PORT), connectionParams, new PacketDispatcherImpl(null), factory);
      connector.connect();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertFalse(firedKeepAliveNotification);

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      //connector.disconnect();

      verify(factory);
   }

   public void testKeepAliveWithClientNotResponding() throws Throwable
   {
      final KeepAliveHandler factory = new ClientKeepAliveFactoryNotResponding();

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
      messagingServer.getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      connectionParams.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);

      LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
      MinaConnector connector = new MinaConnector(location, connectionParams, new PacketDispatcherImpl(null), factory);

      NIOSession session = connector.connect();
      RemotingConnection remotingConnection =  new RemotingConnectionImpl(location, connectionParams, connector);
      createConnection(messagingServer, remotingConnection);
      long clientSessionID = session.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);
      assertNotNull(clientSessionIDNotResponding[0]);
      assertEquals(clientSessionID, clientSessionIDNotResponding[0]);

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      connector.disconnect();
   }

   public void testKeepAliveWithClientTooLongToRespond() throws Throwable
   {
      KeepAliveHandler factory = new KeepAliveHandler()
      {
         public boolean isAlive(Ping ping, Pong pong)
         {
            return false;  //todo
         }

         public void handleDeath(long sessionId)
         {
            //todo
         }

         public Pong ping(Ping pong)
         {
            try
            {
               wait(2 * 3600);
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            return new Pong(randomLong(), false);
         }
      };

      try
      {
         ConnectionParams connectionParams = new ConnectionParamsImpl();
         connectionParams.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
         connectionParams.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
         LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
         MinaConnector connector = new MinaConnector(location, connectionParams,
                 new PacketDispatcherImpl(null), factory);

         NIOSession session = connector.connect();
         //create a connection properly to initiate ping
         RemotingConnection remotingConnection =  new RemotingConnectionImpl(location, connectionParams, connector);
         createConnection(messagingServer, remotingConnection);
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
         messagingServer.getRemotingService().addRemotingSessionListener(listener);

         boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
                 + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
         assertTrue("notification has not been received", firedKeepAliveNotification);
         assertEquals(clientSessionID, clientSessionIDNotResponding.longValue());

         messagingServer.getRemotingService().removeRemotingSessionListener(listener);
         connector.disconnect();

      }
      finally
      {
         // test is done: wake up the factory
         synchronized (factory)
         {
            factory.notify();
         }
      }
   }

   public void testKeepAliveWithClientRespondingAndClientNotResponding()
           throws Throwable
   {
      KeepAliveHandler notRespondingfactory = new ClientKeepAliveFactoryNotResponding();
      KeepAliveHandler respondingfactory = new ClientKeepAliveHandler();

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
      messagingServer.getRemotingService().addRemotingSessionListener(listener);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      connectionParams.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      LocationImpl location = new LocationImpl(TCP, "localhost", TestSupport.PORT);
      MinaConnector connectorNotResponding = new MinaConnector(location, new PacketDispatcherImpl(null), notRespondingfactory);
      MinaConnector connectorResponding = new MinaConnector(location, new PacketDispatcherImpl(null), respondingfactory);

      NIOSession sessionNotResponding = connectorNotResponding.connect();
      //create a connection properly to initiate ping
      RemotingConnection remotingConnection =  new RemotingConnectionImpl(location, connectionParams, connectorNotResponding);
         createConnection(messagingServer, remotingConnection);
      long clientSessionIDNotResponding = sessionNotResponding.getID();


      NIOSession sessionResponding = connectorResponding.connect();
      RemotingConnection remotingConnection2 =  new RemotingConnectionImpl(location, connectionParams, connectorNotResponding);
         createConnection(messagingServer, remotingConnection2);
      long clientSessionIDResponding = sessionResponding.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);

      assertEquals(clientSessionIDNotResponding, sessionIDNotResponding.longValue());
      assertNotSame(clientSessionIDResponding, sessionIDNotResponding.longValue());

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      connectorNotResponding.disconnect();
      connectorResponding.disconnect();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void createConnection(MessagingServer server, RemotingConnection remotingConnection) throws Throwable
   {
      long sessionID = remotingConnection.getSessionID();

      CreateConnectionRequest request =
              new CreateConnectionRequest(server.getVersion().getIncrementingVersion(), sessionID, null, null);

      CreateConnectionResponse response =
              (CreateConnectionResponse) remotingConnection.sendBlocking(0, 0, request);
   }
   // Inner classes -------------------------------------------------

   private class ClientKeepAliveFactoryNotResponding extends ClientKeepAliveHandler
   {
      public Pong ping(Ping ping)
      {
         return null;
      }
   }
}