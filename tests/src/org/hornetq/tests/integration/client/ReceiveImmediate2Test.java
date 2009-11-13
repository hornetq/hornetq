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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReceiveImmediate2Test extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ReceiveImmediate2Test.class);

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ReceiveImmediateTest.queue");
   
   private final SimpleString ADDRESS = new SimpleString("ReceiveImmediateTest.address");
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(false);
      config.getInterceptorClassNames().add(DropForceConsumerDeliveryInterceptor.class.getName());
      server = HornetQ.newHornetQServer(config, false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   private ClientSessionFactory sf;

   public void testConsumerReceiveImmediateDoesNotHang() throws Exception
   {
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnAcknowledge(true);
      sf.setAckBatchSize(0);
      sf.setReconnectAttempts(1);
      sf.setFailoverOnServerShutdown(true);
      
      final ClientSession session = sf.createSession(false, true, false);

      session.createQueue(ADDRESS, QUEUE, null, false);

      final ClientConsumer consumer = session.createConsumer(QUEUE, null, false);
      session.start();
      
      Runnable r = new Runnable()
      {
         
         public void run()
         {
            try
            {
               Thread.sleep(2000);
               //((ClientSessionInternal)session).getConnection().fail(new HornetQException(HornetQException.NOT_CONNECTED));
               //session.close();
               consumer.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

     long start = System.currentTimeMillis();

      new Thread(r).start();
      
      ClientMessage message = consumer.receiveImmediate();
      assertNull(message);

      long end = System.currentTimeMillis();
      assertTrue(end - start >= 2000);
      session.close();

      sf.close();
   }
}
