/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.mina.ServerKeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ServerKeepAliveTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MinaService service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();
      service = null;
   }

   public void testKeepAliveWithServerNotResponding() throws Exception
   {
      ServerKeepAliveFactory factory = new ServerKeepAliveFactory()
      {
         // server does not send ping
         @Override
         public Ping ping(long sessionID)
         {
            return null;
         }

         @Override
         public Pong pong(long sessionID, Ping ping)
         {
            // no pong -> server is not responding
            super.pong(sessionID, ping);
            return null;
         }
      };

      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration(
            "localhost", TestSupport.PORT);
      config.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      config.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      service = new MinaService(config, factory);
      service.start();

      MinaConnector connector = new MinaConnector(service
            .getConfiguration().getLocation(), service.getConfiguration().getConnectionParams(), new PacketDispatcherImpl(null));

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
      connector.addSessionListener(listener);

      NIOSession session = connector.connect();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
            + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(firedKeepAliveNotification);
      assertEquals(session.getID(), sessionIDNotResponding.longValue());

      connector.removeSessionListener(listener);
      connector.disconnect();
   }

   public void testKeepAliveWithServerSessionFailed() throws Exception
   {
      ServerKeepAliveFactory factory = new ServerKeepAliveFactory()
      {
         // server does not send ping
         @Override
         public Ping ping(long sessionID)
         {
            return null;
         }

         @Override
         public Pong pong(long sessionID, Ping ping)
         {
            // no pong -> server is not responding
            super.pong(sessionID, ping);
            return new Pong(sessionID, true);
         }
      };

      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration(
            "localhost", TestSupport.PORT);
      config.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      config.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      service = new MinaService(config, factory);
      service.start();

      MinaConnector connector = new MinaConnector(service
            .getConfiguration().getLocation(), new PacketDispatcherImpl(null));

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
      connector.addSessionListener(listener);

      NIOSession session = connector.connect();

      boolean firedKeepAliveNotification = latch.await(TestSupport.KEEP_ALIVE_INTERVAL
            + TestSupport.KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(firedKeepAliveNotification);
      assertEquals(session.getID(), sessionIDNotResponding.longValue());

      connector.removeSessionListener(listener);
      connector.disconnect();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}