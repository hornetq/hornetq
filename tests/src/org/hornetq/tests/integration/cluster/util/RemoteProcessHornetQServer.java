/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.server.HornetQServer;

/**
 * A RemoteProcessHornetQServer
 *
 * @author jmesnil
 *
 *
 */
public class RemoteProcessHornetQServer implements TestableServer
{

   private String configurationClassName;
   private Process serverProcess;
   private boolean initialised = false;
   private CountDownLatch initLatch;
   private boolean started;

   public RemoteProcessHornetQServer(String configurationClassName)
   {
      this.configurationClassName = configurationClassName;
   }
   
   public boolean isInitialised()
   {
      if (serverProcess == null)
      {
         return false;
      }
      try
      {
         initLatch = new CountDownLatch(1);         
         RemoteProcessHornetQServerSupport.isInitialised(serverProcess);
         boolean ok = initLatch.await(10, TimeUnit.SECONDS);
         if (ok)
         {
            return initialised;
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return false;
      }
      return false;
   }

   public void destroy()
   {
      if(serverProcess != null)
      {
         serverProcess.destroy();
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   public void addInterceptor(Interceptor interceptor)
   {
      throw new UnsupportedOperationException("can't do this with a remote server");
   }

   public void removeInterceptor(Interceptor interceptor)
   {
      throw new UnsupportedOperationException("can't do this with a remote server");
   }

   public void setInitialised(boolean initialised)
   {
      this.initialised = initialised;
      initLatch.countDown();
   }

   public void start() throws Exception
   {
      serverProcess = RemoteProcessHornetQServerSupport.start(configurationClassName, this);
      Thread.sleep(2000);
   }

   public void stop() throws Exception
   {
      if (serverProcess != null)
      {
         RemoteProcessHornetQServerSupport.stop(serverProcess);
         serverProcess = null;
         Thread.sleep(2000);
      }
   }

   public void crash(ClientSession... sessions) throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(sessions.length);

      class MyListener implements SessionFailureListener
      {
         public void connectionFailed(final HornetQException me, boolean failedOver)
         {
            latch.countDown();
         }

         public void beforeReconnect(HornetQException exception)
         {
         }
      }
      for (ClientSession session : sessions)
      {
         session.addFailureListener(new MyListener());
      }

      if (serverProcess != null)
      {
         RemoteProcessHornetQServerSupport.crash(serverProcess);
         serverProcess = null;
      }
      
      // Wait to be informed of failure
      boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }



   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public void setStarted(boolean init)
   {
      started = true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.util.TestableServer#getServer()
    */
   public HornetQServer getServer()
   {
      return null;
   }
}
