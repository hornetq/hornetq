/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_INTERVAL;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.ConnectionExceptionListener;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ClientKeepAliveTest extends TestCase
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
      RemotingConfiguration remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
      remotingConfig.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
      remotingConfig.setKeepAliveTimeout(KEEP_ALIVE_TIMEOUT);
      service = new MinaService(remotingConfig);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();
      service = null;
   }

   public void testKeepAliveWithClientOK() throws Exception
   {
      KeepAliveFactory factory = createMock(KeepAliveFactory.class);

      // client never send ping
      expect(factory.ping()).andStubReturn(null);
      // client is responding
      expect(factory.pong()).andReturn(new Pong()).atLeastOnce();

      replay(factory);

      
      final CountDownLatch latch = new CountDownLatch(1);

      service.setConnectionExceptionListener(new ConnectionExceptionListener()
      {
         public void handleConnectionException(Exception e, String sessionID)
         {
            latch.countDown();
         }
      });

      MinaConnector connector = new MinaConnector(service.getRemotingConfiguration(), factory);
      connector.connect();

      boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
            + KEEP_ALIVE_TIMEOUT + 1, SECONDS);
      assertFalse(firedKeepAliveNotification);

      connector.disconnect();

      verify(factory);
   }
   
   public void testKeepAliveWithClientNotResponding() throws Exception
   {
      KeepAliveFactory factory = createMock(KeepAliveFactory.class);

      // client never send ping
      expect(factory.ping()).andStubReturn(null);
      // no pong -> client is not responding
      expect(factory.pong()).andReturn(null).atLeastOnce();

      replay(factory);

      final String[] clientSessionIDNotResponding = new String[1];
      final CountDownLatch latch = new CountDownLatch(1);

      service.setConnectionExceptionListener(new ConnectionExceptionListener()
      {
         public void handleConnectionException(Exception e, String sessionID)
         {
            clientSessionIDNotResponding[0] = sessionID;
            latch.countDown();
         }
      });
      
      MinaConnector connector = new MinaConnector(service.getRemotingConfiguration(), factory);

      NIOSession session = connector.connect();
      String clientSessionID = session.getID();

      boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
            + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);
      assertNotNull(clientSessionIDNotResponding[0]);
      assertEquals(clientSessionID, clientSessionIDNotResponding[0]);

      connector.disconnect();

      verify(factory);
   }

   public void testKeepAliveWithClientTooLongToRespond() throws Exception
   {
      KeepAliveFactory factory = new KeepAliveFactory()
      {
         public Ping ping()
         {
            return null;
         }

         public synchronized Pong pong()
         {
            // like a TCP timeout, there is no response in the next 2 hours
            try
            {
               wait(2 * 3600);
            } catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            return new Pong();
         }
      };

      try
      {
         MinaConnector connector = new MinaConnector(service.getRemotingConfiguration(),
               factory);

         NIOSession session = connector.connect();
         String clientSessionID = session.getID();

         final String[] clientSessionIDNotResponding = new String[1];
         final CountDownLatch latch = new CountDownLatch(1);

         service.setConnectionExceptionListener(new ConnectionExceptionListener()
         {
            public void handleConnectionException(Exception e, String sessionID)
            {
               clientSessionIDNotResponding[0] = sessionID;
               latch.countDown();
            }
         });

         boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
               + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
         assertTrue("notification has not been received", firedKeepAliveNotification);
         assertNotNull(clientSessionIDNotResponding[0]);
         assertEquals(clientSessionID, clientSessionIDNotResponding[0]);

         connector.disconnect();

      } finally
      {
         // test is done: wake up the factory
         synchronized (factory)
         {
            factory.notify();
         }
      }
   }

   public void testKeepAliveWithClientRespondingAndClientNotResponding()
         throws Exception
   {
      KeepAliveFactory notRespondingfactory = createMock(KeepAliveFactory.class);
      expect(notRespondingfactory.ping()).andStubReturn(null);
      expect(notRespondingfactory.pong()).andReturn(null).atLeastOnce();

      KeepAliveFactory respondingfactory = createMock(KeepAliveFactory.class);
      expect(respondingfactory.ping()).andStubReturn(null);
      expect(respondingfactory.pong()).andReturn(new Pong()).atLeastOnce();

      replay(notRespondingfactory, respondingfactory);

      final String[] sessionIDNotResponding = new String[1];
      final CountDownLatch latch = new CountDownLatch(1);

      service.setConnectionExceptionListener(new ConnectionExceptionListener()
      {
         public void handleConnectionException(Exception e, String sessionID)
         {
            sessionIDNotResponding[0] = sessionID;
            latch.countDown();
         }
      });
      
      MinaConnector connectorNotResponding = new MinaConnector(service
            .getRemotingConfiguration(), notRespondingfactory);
      MinaConnector connectorResponding = new MinaConnector(service
            .getRemotingConfiguration(), respondingfactory);

      NIOSession sessionNotResponding = connectorNotResponding.connect();
      String clientSessionIDNotResponding = sessionNotResponding.getID();

      
      NIOSession sessionResponding = connectorResponding.connect();
      String clientSessionIDResponding = sessionResponding.getID();

      boolean firedKeepAliveNotification = latch.await(KEEP_ALIVE_INTERVAL
            + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue("notification has not been received", firedKeepAliveNotification);

      assertNotNull(sessionIDNotResponding[0]);
      assertEquals(clientSessionIDNotResponding, sessionIDNotResponding[0]);
      assertNotSame(clientSessionIDResponding, sessionIDNotResponding[0]);

      connectorNotResponding.disconnect();
      connectorResponding.disconnect();

      verify(notRespondingfactory, respondingfactory);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}