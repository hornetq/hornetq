/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_INTERVAL;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.mina.ServerKeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

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
         public Ping ping(String sessionID)
         {
            return null;
         }

         @Override
         public Pong pong(String sessionID, Ping ping)
         {
            // no pong -> server is not responding
            super.pong(sessionID, ping);
            return null;
         }
      };

      Configuration config = ConfigurationHelper.newConfiguration(TCP,
            "localhost", PORT);
      config.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
      config.setKeepAliveTimeout(KEEP_ALIVE_TIMEOUT);
      service = new MinaService(config, factory);
      service.start();

      MinaConnector connector = new MinaConnector(service
            .getConfiguration(), new PacketDispatcherImpl());
      final String[] sessionIDNotResponding = new String[1];
      final CountDownLatch latch = new CountDownLatch(1);

      FailureListener listener = new FailureListener()
      {
         public void onFailure(MessagingException me)
         {
            assertTrue(me instanceof RemotingException);
            RemotingException re = (RemotingException) me;
            sessionIDNotResponding[0] = re.getSessionID();
            latch.countDown();
         }
      };
      connector.addFailureListener(listener);

      NIOSession session = connector.connect();

      boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
            + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(firedKeepAliveNotification);
      assertEquals(session.getID(), sessionIDNotResponding[0]);

      connector.removeFailureListener(listener);
      connector.disconnect();
   }

   public void testKeepAliveWithServerSessionFailed() throws Exception
   {
      ServerKeepAliveFactory factory = new ServerKeepAliveFactory()
      {
         // server does not send ping
         @Override
         public Ping ping(String sessionID)
         {
            return null;
         }

         @Override
         public Pong pong(String sessionID, Ping ping)
         {
            // no pong -> server is not responding
            super.pong(sessionID, ping);
            return new Pong(sessionID, true);
         }
      };

      Configuration config = ConfigurationHelper.newConfiguration(TCP,
            "localhost", PORT);
      config.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
      config.setKeepAliveTimeout(KEEP_ALIVE_TIMEOUT);
      service = new MinaService(config, factory);
      service.start();

      MinaConnector connector = new MinaConnector(service
            .getConfiguration(), new PacketDispatcherImpl());
      final String[] sessionIDNotResponding = new String[1];
      final CountDownLatch latch = new CountDownLatch(1);

      FailureListener listener = new FailureListener()
      {
         public void onFailure(MessagingException me)
         {
            assertTrue(me instanceof RemotingException);
            RemotingException re = (RemotingException) me;
            sessionIDNotResponding[0] = re.getSessionID();
            latch.countDown();
         }
      };
      connector.addFailureListener(listener);

      NIOSession session = connector.connect();

      boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
            + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(firedKeepAliveNotification);
      assertEquals(session.getID(), sessionIDNotResponding[0]);

      connector.removeFailureListener(listener);
      connector.disconnect();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}