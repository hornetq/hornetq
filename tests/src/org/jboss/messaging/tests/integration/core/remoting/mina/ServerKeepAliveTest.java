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
import org.jboss.messaging.core.client.impl.RemotingConnection;
import org.jboss.messaging.core.client.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
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

   private MessagingServer messagingServer;

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
      messagingServer.stop();
      messagingServer = null;
   }

   public void testKeepAliveWithServerNotResponding() throws Throwable
   {
      //set the server timeouts to be twice that of the server to force failure
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration(
            "localhost", TestSupport.PORT);
      config.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL * 2);
      config.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT * 2);
      ConfigurationImpl clientConfig = ConfigurationHelper.newTCPConfiguration(
            "localhost", TestSupport.PORT);
      clientConfig.setKeepAliveInterval(TestSupport.KEEP_ALIVE_INTERVAL);
      clientConfig.setKeepAliveTimeout(TestSupport.KEEP_ALIVE_TIMEOUT);
      messagingServer = new MessagingServerImpl(config);
      messagingServer.start();

      MinaConnector connector = new MinaConnector(clientConfig.getLocation(), clientConfig.getConnectionParams(), new PacketDispatcherImpl(null));

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
      RemotingConnection remotingConnection =  new RemotingConnectionImpl(config.getLocation(), config.getConnectionParams(), connector);
      createConnection(messagingServer, remotingConnection);
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
   private void createConnection(MessagingServer server, RemotingConnection remotingConnection) throws Throwable
   {
      long sessionID = remotingConnection.getSessionID();

      CreateConnectionRequest request =
              new CreateConnectionRequest(server.getVersion().getIncrementingVersion(), sessionID, null, null);

      CreateConnectionResponse response =
              (CreateConnectionResponse) remotingConnection.sendBlocking(0, 0, request);
   }
   // Inner classes -------------------------------------------------
}