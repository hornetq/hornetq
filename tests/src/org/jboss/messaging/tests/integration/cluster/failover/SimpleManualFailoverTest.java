/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SimpleAutomaticFailoverTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleManualFailoverTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(SimpleManualFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService server0Service;

   private MessagingService server1Service;

   private Map<String, Object> server1Params = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private ClientSession sendAndConsume(final ClientSessionFactory sf) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().putString("aardvarks");
         message.getBody().flip();
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);
      
      return session;
   }
   
   public void testFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sendAndConsume(sf);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public boolean connectionFailed(MessagingException me)
         {
            log.info("*** connection failed");
            latch.countDown();
            
            return true;
         }
      }

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      conn.addFailureListener(new MyListener());

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      log.info("closing session");
      session.close();
      log.info("closed session");

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   server1Params));

      session = sendAndConsume(sf);

      session.close();
      
      assertEquals(0, sf.numSessions());
      
      assertEquals(0, sf.numConnections());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      Configuration server1Conf = new ConfigurationImpl();
      server1Conf.setSecurityEnabled(false);
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      server1Conf.getAcceptorConfigurations()
                 .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                 server1Params));
      server1Service = Messaging.newNullStorageMessagingService(server1Conf);
      server1Service.start();

      Configuration server0Conf = new ConfigurationImpl();
      server0Conf.setSecurityEnabled(false);
      server0Conf.getAcceptorConfigurations()
                 .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      server0Service = Messaging.newNullStorageMessagingService(server0Conf);
      server0Service.start();
   }

   protected void tearDown() throws Exception
   {
      server1Service.stop();

      server0Service.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
