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

package org.jboss.messaging.tests.stress.remote;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.integration.remoting.PingTest;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class PingStressTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PingTest.class);

   private static final long PING_INTERVAL = 500;

   // Attributes ----------------------------------------------------

   private MessagingService messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      messagingService = createService(false, config);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();
      super.tearDown();
   }

   protected int getNumberOfIterations()
   {
      System.out.println("Change PingStressTest::getNumberOfIterations to enable this test");
      return 0;
   }

   public void testMultiThreadOpenAndCloses() throws Exception
   {
      for (int i = 0; i < getNumberOfIterations(); i++)
      {
         System.out.println("Run " + i);
         internalTest();
         tearDown();
         setUp();
      }

   }

   /*
    * Test the client triggering failure due to no pong received in time
    */
   private void internalTest() throws Exception
   {
      final TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");

      Interceptor noPongInterceptor = new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection conn) throws MessagingException
         {
            log.info("In interceptor, packet is " + packet.getType());
            if (packet.getType() == PacketImpl.PING)
            {
               log.info("Ignoring Ping packet.. it will be dropped");
               return false;
            }
            else
            {
               return true;
            }
         }
      };

      messagingService.getServer().getRemotingService().addInterceptor(noPongInterceptor);

      final ClientSessionFactory csf1 = new ClientSessionFactoryImpl(transportConfig,
                                                                     null,
                                                                     DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                                     PING_INTERVAL,
                                                                     (long)(PING_INTERVAL * 1.5),
                                                                     PING_INTERVAL * 4, // Using a smaller call timeout
                                                                                        // for this test
                                                                     DEFAULT_CONSUMER_WINDOW_SIZE,
                                                                     DEFAULT_CONSUMER_MAX_RATE,
                                                                     DEFAULT_SEND_WINDOW_SIZE,
                                                                     DEFAULT_PRODUCER_MAX_RATE,
                                                                     DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                     DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                                     DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                                     DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                                     DEFAULT_AUTO_GROUP,
                                                                     DEFAULT_MAX_CONNECTIONS,
                                                                     DEFAULT_PRE_ACKNOWLEDGE,
                                                                     DEFAULT_ACK_BATCH_SIZE,
                                                                     DEFAULT_RETRY_INTERVAL,
                                                                     DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                     DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                                     DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

      final int numberOfSessions = 1;
      final int numberOfThreads = 30;

      final CountDownLatch flagStart = new CountDownLatch(1);
      final CountDownLatch flagAligned = new CountDownLatch(numberOfThreads);

      class LocalThread extends Thread
      {
         Throwable failure;

         int threadNumber;

         public LocalThread(final int i)
         {
            super("LocalThread i = " + i);
            threadNumber = i;
         }

         @Override
         public void run()
         {
            try
            {

               final ClientSessionFactory csf2 = new ClientSessionFactoryImpl(transportConfig,
                                                                              null,
                                                                              DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                                              PING_INTERVAL,
                                                                              (long)(PING_INTERVAL * 1.5),
                                                                              PING_INTERVAL * 4, // Using a smaller call
                                                                                                 // timeout for this
                                                                                                 // test
                                                                              DEFAULT_CONSUMER_WINDOW_SIZE,
                                                                              DEFAULT_CONSUMER_MAX_RATE,
                                                                              DEFAULT_SEND_WINDOW_SIZE,
                                                                              DEFAULT_PRODUCER_MAX_RATE,
                                                                              DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                              DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                                              DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                                              DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                                              DEFAULT_AUTO_GROUP,
                                                                              DEFAULT_MAX_CONNECTIONS,
                                                                              DEFAULT_PRE_ACKNOWLEDGE,
                                                                              DEFAULT_ACK_BATCH_SIZE,
                                                                              DEFAULT_RETRY_INTERVAL,
                                                                              DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                              DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                                              DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

               // Start all at once to make concurrency worst
               flagAligned.countDown();
               flagStart.await();
               for (int i = 0; i < numberOfSessions; i++)
               {
                  System.out.println(getName() + " Session = " + i);

                  ClientSession session;

                  // Sometimes we use the SessionFactory declared on this thread, sometimes the SessionFactory declared
                  // on the test, sharing it with other tests
                  // (playing a possible user behaviour where you share the Factories among threads, versus not sharing
                  // them)
                  if (RandomUtil.randomBoolean())
                  {
                     session = csf1.createSession(false, false, false);
                  }
                  else
                  {
                     session = csf2.createSession(false, false, false);
                  }

                  // We will wait to anything between 0 to PING_INTERVAL * 2
                  Thread.sleep(PING_INTERVAL * (threadNumber % 3));

                  session.close();
               }
            }
            catch (Throwable e)
            {
               failure = e;
            }
         }
      };

      LocalThread threads[] = new LocalThread[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         threads[i] = new LocalThread(i);
         threads[i].start();
      }

      flagAligned.await();
      flagStart.countDown();

      Throwable e = null;
      for (LocalThread t : threads)
      {
         t.join();
         if (t.failure != null)
         {
            e = t.failure;
         }
      }

      if (e != null)
      {
         throw new Exception("Test Failed", e);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}