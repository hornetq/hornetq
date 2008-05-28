/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.NIOSession;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.ClientKeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.CountDownLatch;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.atomic.AtomicLong;

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
      ClientKeepAliveFactory factory = new ClientKeepAliveFactory();

      // client never send ping
      //expect(factory.pong(0, isA(Ping.class))).andStubReturn(new Pong());

      ///replay(factory);

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
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2000, MILLISECONDS);
      assertFalse(firedKeepAliveNotification);

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      //connector.disconnect();

      // verify(factory);
   }

   public void testKeepAliveWithClientNotResponding() throws Throwable
   {
      final ClientKeepAliveFactory factory = new ClientKeepAliveFactoryNotResponding();

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
      long clientSessionID = session.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2000, MILLISECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);
      assertNotNull(clientSessionIDNotResponding[0]);
      //assertEquals(clientSessionID, clientSessionIDNotResponding[0]);

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      connector.disconnect();
   }

   public void testKeepAliveWithClientTooLongToRespond() throws Throwable
   {
      ClientKeepAliveFactory factory = new ClientKeepAliveFactory()
      {

         public Pong pong(long sessionID, Ping ping)
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
                 + TestSupport.KEEP_ALIVE_TIMEOUT + 2000, MILLISECONDS);
         assertTrue("notification has not been received", firedKeepAliveNotification);
         //assertEquals(clientSessionID, clientSessionIDNotResponding.longValue());

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
      ClientKeepAliveFactory notRespondingfactory = new ClientKeepAliveFactoryNotResponding();
      ClientKeepAliveFactory respondingfactory = new ClientKeepAliveFactory();

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
      long clientSessionIDNotResponding = sessionNotResponding.getID();


      NIOSession sessionResponding = connectorResponding.connect();
      long clientSessionIDResponding = sessionResponding.getID();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
              + TestSupport.KEEP_ALIVE_TIMEOUT + 2000, MILLISECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);

      //assertEquals(clientSessionIDNotResponding, sessionIDNotResponding.longValue());
      assertNotSame(clientSessionIDResponding, sessionIDNotResponding.longValue());

      messagingServer.getRemotingService().removeRemotingSessionListener(listener);
      connectorNotResponding.disconnect();
      connectorResponding.disconnect();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class ClientKeepAliveFactoryNotResponding extends ClientKeepAliveFactory
   {
      public Pong pong(long sessionID, Ping ping)
      {
         return null;
      }
   }
}