/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   private HornetQServer server0Service;

   private HornetQServer server1Service;

   private Map<String, Object> server1Params = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private ClientSession sendAndConsume(final ClientSessionFactory sf) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);
      
      return session;
   }
   
   public void testFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sendAndConsume(sf);

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(HornetQException me)
         {            
            latch.countDown();
         }
      }

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      conn.addFailureListener(new MyListener());

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      session.close();
 
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
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
      super.setUp();
      
      Configuration server1Conf = new ConfigurationImpl();
      server1Conf.setSecurityEnabled(false);
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      server1Conf.getAcceptorConfigurations()
                 .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                 server1Params));
      server1Service = HornetQ.newHornetQServer(server1Conf, false);
      server1Service.start();

      Configuration server0Conf = new ConfigurationImpl();
      server0Conf.setSecurityEnabled(false);
      server0Conf.getAcceptorConfigurations()
                 .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      server0Service = HornetQ.newHornetQServer(server0Conf, false);
      server0Service.start();
   }

   protected void tearDown() throws Exception
   {
      server1Service.stop();

      server0Service.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      server1Service = null;
      
      server0Service = null;
      
      server1Params = null;
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
