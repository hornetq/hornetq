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

package org.hornetq.tests.stress.remote;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.remoting.PingTest;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

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

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      server = createServer(false, config);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null && server.isStarted())
      {
         server.stop();
         server = null;
      }
      super.tearDown();
   }

   protected int getNumberOfIterations()
   {
      return 20;
   }

   public void testMultiThreadOpenAndCloses() throws Exception
   {
      for (int i = 0; i < getNumberOfIterations(); i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }
         System.out.println("Run " + i);
         internalTest();
      }

   }

   /*
    * Test the client triggering failure due to no pong received in time
    */
   private void internalTest() throws Exception
   {
      final TransportConfiguration transportConfig = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");

      Interceptor noPongInterceptor = new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection conn) throws HornetQException
         {
            PingStressTest.log.info("In interceptor, packet is " + packet.getType());
            if (packet.getType() == PacketImpl.PING)
            {
               PingStressTest.log.info("Ignoring Ping packet.. it will be dropped");
               return false;
            }
            else
            {
               return true;
            }
         }
      };

      server.getRemotingService().addInterceptor(noPongInterceptor);
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(transportConfig);
      final ClientSessionFactory csf1 = locator.createSessionFactory();

      csf1.getServerLocator().setClientFailureCheckPeriod(PingStressTest.PING_INTERVAL);
      csf1.getServerLocator().setConnectionTTL((long)(PingStressTest.PING_INTERVAL * 1.5));
      csf1.getServerLocator().setCallTimeout(PingStressTest.PING_INTERVAL * 10);

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

               ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(transportConfig);
               final ClientSessionFactory csf2 = locator.createSessionFactory();

               csf2.getServerLocator().setClientFailureCheckPeriod(PingStressTest.PING_INTERVAL);
               csf2.getServerLocator().setConnectionTTL((long)(PingStressTest.PING_INTERVAL * 1.5));
               csf2.getServerLocator().setCallTimeout(PingStressTest.PING_INTERVAL * 10);

               // Start all at once to make concurrency worst
               flagAligned.countDown();
               flagStart.await();
               for (int i = 0; i < numberOfSessions; i++)
               {
                  System.out.println(getName() + " Session = " + i);

                  ClientSession session;

                  // Sometimes we use the SessionFactory declared on this thread, sometimes the SessionFactory declared
                  // on the test, sharing it with other threads
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
                  Thread.sleep(PingStressTest.PING_INTERVAL * (threadNumber % 3));

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