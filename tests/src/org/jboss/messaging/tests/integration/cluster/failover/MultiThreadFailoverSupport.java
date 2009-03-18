/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A MultiThreadFailoverSupport
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Mar 17, 2009 11:15:02 AM
 *
 *
 */
public abstract class MultiThreadFailoverSupport extends UnitTestCase
{

   // Constants -----------------------------------------------------
   
   private final Logger log = Logger.getLogger(this.getClass());

   // Attributes ----------------------------------------------------

   protected Timer timer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract void start() throws Exception;

   protected abstract void stop() throws Exception;
   
   protected abstract ClientSessionFactoryInternal createSessionFactory();
   
   protected void setUp() throws Exception
   {
      super.setUp();
      timer = new Timer();
   }
   
   protected void tearDown() throws Exception
   {
      timer.cancel();
      super.tearDown();
   }
   
   protected boolean shouldFail()
   {
      return true;
   }


   
   protected void runMultipleThreadsFailoverTest(final RunnableT runnable,
                                       final int numThreads,
                                       final int numIts,
                                       final boolean failOnCreateConnection,
                                       final long failDelay) throws Exception
   {
      for (int its = 0; its < numIts; its++)
      {
         log.info("************ ITERATION: " + its);

         start();

         final ClientSessionFactoryInternal sf = createSessionFactory();

         final ClientSession session = sf.createSession(false, true, true);

         Failer failer = startFailer(failDelay, session, failOnCreateConnection);

         class Runner extends Thread
         {
            private volatile Throwable throwable;

            private final RunnableT test;

            private final int threadNum;

            Runner(final RunnableT test, final int threadNum)
            {
               this.test = test;

               this.threadNum = threadNum;
            }

            @Override
            public void run()
            {
               try
               {
                  test.run(sf, threadNum);
               }
               catch (Throwable t)
               {
                  throwable = t;

                  log.error("Failed to run test", t);

                  // Case a failure happened here, it should print the Thread dump
                  // Sending it to System.out, as it would show on the Tests report
                  System.out.println(threadDump(" - fired by MultiThreadRandomFailoverTestBase::runTestMultipleThreads (" + t.getLocalizedMessage() + ")"));
               }
            }
         }

         do
         {
            List<Runner> threads = new ArrayList<Runner>();

            for (int i = 0; i < numThreads; i++)
            {
               Runner runner = new Runner(runnable, i);

               threads.add(runner);

               runner.start();
            }

            for (Runner thread : threads)
            {
               thread.join();

               if (thread.throwable != null)
               {
                  throw new Exception("Exception on thread " + thread, thread.throwable);
               }
            }

            log.info("completed loop");

            runnable.checkFail();

         }
         while (!failer.isExecuted());

         InVMConnector.resetFailures();

         log.info("closing session");
         session.close();
         log.info("closed session");

         assertEquals(0, sf.numSessions());

         assertEquals(0, sf.numConnections());

         log.info("stopping");
         stop();
         log.info("stopped");
      }
   }


   // Private -------------------------------------------------------

   private Failer startFailer(final long time, final ClientSession session, final boolean failOnCreateConnection)
   {
      Failer failer = new Failer(session, failOnCreateConnection);

      // This is useful for debugging.. just change shouldFail to return false, and Failer will not be executed
      if (shouldFail())
      {
         timer.schedule(failer, (long)(time * Math.random()), 100);
      }

      return failer;
   }


   // Inner classes -------------------------------------------------

 
   protected abstract class RunnableT extends Thread
   {
      private volatile String failReason;

      private volatile Throwable throwable;

      public void setFailed(final String reason, final Throwable throwable)
      {
         failReason = reason;
         this.throwable = throwable;
      }

      public void checkFail()
      {
         if (throwable != null)
         {
            log.error("Test failed: " + failReason, throwable);
         }
         if (failReason != null)
         {
            fail(failReason);
         }
      }

      public abstract void run(final ClientSessionFactory sf, final int threadNum) throws Exception;
   }

   

   private class Failer extends TimerTask
   {
      private final ClientSession session;

      private boolean executed;

      private final boolean failOnCreateConnection;

      public Failer(final ClientSession session, final boolean failOnCreateConnection)
      {
         this.session = session;

         this.failOnCreateConnection = failOnCreateConnection;
      }

      @Override
      public synchronized void run()
      {
         log.info("** Failing connection");

         RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();

         if (failOnCreateConnection)
         {
            InVMConnector.numberOfFailures = 1;
            InVMConnector.failOnCreateConnection = true;
         }
         else
         {
            conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
         }

         log.info("** Fail complete");

         cancel();

         executed = true;
      }

      public synchronized boolean isExecuted()
      {
         log.info("executed??" + executed);
         return executed;
      }
   }

   

}
