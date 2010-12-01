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

package org.hornetq.tests.integration.remoting;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ReconnectSimpleTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReconnectTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReconnectTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReconnectNetty() throws Exception
   {
      internalTestReconnect(true);
   }

   public void testReconnectInVM() throws Exception
   {
      internalTestReconnect(false);
   }

   public void internalTestReconnect(final boolean isNetty) throws Exception
   {
      final int pingPeriod = 1000;

      HornetQServer server = createServer(false, isNetty);

      server.start();

      ClientSessionInternal session = null;

      try
      {
         ServerLocator locator = createFactory(isNetty);
         locator.setClientFailureCheckPeriod(pingPeriod);
         locator.setRetryInterval(500);
         locator.setRetryIntervalMultiplier(1d);
         locator.setReconnectAttempts(-1);
         locator.setConfirmationWindowSize(1024 * 1024);
         ClientSessionFactory factory = locator.createSessionFactory();


         session = (ClientSessionInternal)factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);

         final CountDownLatch latch = new CountDownLatch(1);

         session.addFailureListener(new SessionFailureListener()
         {

            public void connectionFailed(final HornetQException me, boolean failedOver)
            {
               count.incrementAndGet();
               latch.countDown();
            }

            public void beforeReconnect(final HornetQException exception)
            {
            }

         });

         server.stop();

         Thread.sleep((pingPeriod * 2));

         server.start();

         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

         // Some time to let possible loops to occur
         Thread.sleep(500);

         Assert.assertEquals(1, count.get());

         locator.close();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable e)
         {
         }

         server.stop();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
