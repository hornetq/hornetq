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

import java.util.concurrent.CountDownLatch;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.SimpleString;

/**
 * A LargeMessageFailoverTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 8, 2008 7:09:38 PM
 *
 *
 */
public class JustReplicationTest extends FailoverTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFactory() throws Exception
   {
      final ClientSessionFactory factory = createFailoverFactory(); // Just a regular factory with Backup configured
      
      final int NUMBER_OF_THREADS = 5;
      final int NUMBER_OF_ITERATIONS = 5;
      final int NUMBER_OF_SESSIONS = 5;
      
      final CountDownLatch flagAlign = new CountDownLatch(NUMBER_OF_THREADS);
      final CountDownLatch flagStart = new CountDownLatch(1);

      class LocalThread extends Thread
      {

         Throwable e;

         @Override
         public void run()
         {
            try
            {
               flagAlign.countDown();
               flagStart.await();

               
               for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
               {
                  ClientSession sessions[] = new ClientSession[NUMBER_OF_SESSIONS];
                  for (int j = 0; j < NUMBER_OF_SESSIONS; j++)
                  {
                     sessions[j] = factory.createSession(false, true, true);
                     sessions[j].start();
                  }

                  for (ClientSession session : sessions)
                  {
                     session.close();
                  }

               }
            }
            catch (Throwable e)
            {
               this.e = e;
            }
         }
      }

      LocalThread t[] = new LocalThread[NUMBER_OF_THREADS];

      for (int i = 0; i < t.length; i++)
      {
         t[i] = new LocalThread();
         t[i].start();
      }
      
      flagAlign.await();
      flagStart.countDown();

      for (LocalThread localT : t)
      {
         localT.join();
      }

      for (LocalThread localT : t)
      {
         if (localT.e != null)
         {
            throw new Exception(localT.e.getMessage(), localT.e);
         }
      }

   }

   public void testJustReplication() throws Exception
   {
      ClientSessionFactory factory = createFailoverFactory();
      factory.setBlockOnAcknowledge(true);
      factory.setBlockOnNonPersistentSend(true);
      factory.setBlockOnPersistentSend(true);

      factory.setMinLargeMessageSize(10 * 1024);

      ClientSession session = factory.createSession(null, null, false, true, true, false, 0);

      final int numberOfMessages = 500;

      final int numberOfBytes = 15000;

      try
      {
         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         // he remotingConnection could be used to force a failure
         // final RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createClientMessage(true);

            MessagingBuffer buffer = ChannelBuffers.buffer(15000);
            buffer.setInt(0, i);
            buffer.writerIndex(buffer.capacity());
            
            message.setBody(buffer);

            producer.send(message);

         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = consumer.receive(5000);

            assertNotNull(message);

            message.acknowledge();

            MessagingBuffer buffer = message.getBody();

            assertEquals(numberOfBytes, buffer.writerIndex());

            assertEquals(i, buffer.readInt());
         }

         assertNull(consumer.receive(500));
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Exception ignored)
         {
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      setUpFileBased(100 * 1024 * 1024);
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
