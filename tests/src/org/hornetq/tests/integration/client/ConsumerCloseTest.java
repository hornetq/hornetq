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
package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ConsumerCloseTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConsumerCloseTest.class);


   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;

   private SimpleString queue;

   private SimpleString address;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCanNotUseAClosedConsumer() throws Exception
   {
      final ClientConsumer consumer = session.createConsumer(queue);

      consumer.close();

      assertTrue(consumer.isClosed());

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.receive();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.receiveImmediate();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.setMessageHandler(new MessageHandler()
            {
               public void onMessage(ClientMessage message)
               {
               }
            });
         }
      });
   }

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1526
   public void testCloseWithManyMessagesInBufferAndSlowConsumer() throws Exception
   {
      ClientConsumer consumer = session.createConsumer(queue);

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(false);

         producer.send(message);
      }

      class MyHandler implements MessageHandler
      {
         public void onMessage(ClientMessage message)
         {
            try
            {
               Thread.sleep(1000);
            }
            catch (Exception e)
            {
            }
         }
      }
      
      consumer.setMessageHandler(new MyHandler());
      
      session.start();
      
      Thread.sleep(1000);
      
      //Close shouldn't wait for all messages to be processed before closing
      long start= System.currentTimeMillis();
      consumer.close();
      long end = System.currentTimeMillis();
      
      assertTrue(end - start <= 1500);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = HornetQ.newHornetQServer(config, false);
      server.start();

      address = randomSimpleString();
      queue = randomSimpleString();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.createQueue(address, queue, false);

   }

   private ClientSessionFactory sf;

   @Override
   protected void tearDown() throws Exception
   {
      session.deleteQueue(queue);

      session.close();

      sf.close();

      server.stop();
      
      session = null;
      sf = null;
      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
