/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

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
         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(ADDRESS);

         // he remotingConnection could be used to force a failure
         // final RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createClientMessage(true);

            ByteBuffer buffer = ByteBuffer.allocate(numberOfBytes);

            buffer.putInt(i);

            buffer.rewind();

            message.setBody(new ByteBufferWrapper(buffer));

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

            buffer.rewind();

            assertEquals(numberOfBytes, buffer.limit());

            assertEquals(i, buffer.getInt());
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
