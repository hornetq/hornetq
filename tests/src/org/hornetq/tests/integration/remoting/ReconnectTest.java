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
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestSuite;

import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.FailureListener;
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

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   
   // This is a hack to remove this test from the testsuite
   // Remove this method to enable this Test on the testsuite.
   // You can still run tests individually on eclipse, but not on the testsuite
   public static TestSuite suite()
   {
      TestSuite suite = new TestSuite();
   
      // System.out -> JUnit report
      System.out.println("Test ReconnectTest being ignored for now!");
      
      return suite;
   }
   
   
   public void testReconnectNetty() throws Exception
   {
      internalTestReconnect(true);
   }
   
   public void testReconnectInVM() throws Exception
   {
      internalTestReconnect(false);
   }

   public void internalTestReconnect(boolean isNetty) throws Exception
   {

      HornetQServer server = createServer(false, isNetty);

      server.start();

      ClientSessionInternal session = null;
      
      try
      {

         ClientSessionFactory factory = createFactory(isNetty);

         factory.setClientFailureCheckPeriod(2000);
         factory.setRetryInterval(500);
         factory.setRetryIntervalMultiplier(1d);
         factory.setReconnectAttempts(-1);
         factory.setConfirmationWindowSize(1024 * 1024);

         session = (ClientSessionInternal)factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);
         
         final CountDownLatch latch = new CountDownLatch(1);
         
         session.getConnection().addFailureListener(new FailureListener()
         {

            public void connectionFailed(HornetQException me)
            {
               count.incrementAndGet();
               latch.countDown();
            }
            
         });
         
         server.stop();
         
         // I couldn't find a way to install a latch here as I couldn't just use the FailureListener
         // as the FailureListener won't be informed until the reconnection process is done.
         Thread.sleep(2100);
         
         server.start();
         
         latch.await();

         // Some process to let the Failure listener loop occur
         Thread.sleep(500);
         
         assertEquals(1, count.get());
 
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
