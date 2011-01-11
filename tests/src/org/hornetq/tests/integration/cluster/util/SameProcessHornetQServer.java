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
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;

/**
 * A SameProcessHornetQServer
 *
 * @author jmesnil
 *
 *
 */
public class SameProcessHornetQServer implements TestableServer
{
   
   private HornetQServer server;

   public SameProcessHornetQServer(HornetQServer server)
   {
      this.server = server;
   }

   public boolean isInitialised()
   {
      return server.isInitialised();
   }

   public void destroy()
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public void addInterceptor(Interceptor interceptor)
   {
      server.getRemotingService().addInterceptor(interceptor);
   }

   public void removeInterceptor(Interceptor interceptor)
   {
      server.getRemotingService().removeInterceptor(interceptor);
   }

   public void start() throws Exception
   {
      server.start();
   }

   public void stop() throws Exception
   {
      server.stop();
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
            System.out.println("MyListener.beforeReconnect");
         }
      }
      for (ClientSession session : sessions)
      {
         session.addFailureListener(new MyListener());
      }

      ClusterManagerImpl clusterManager = (ClusterManagerImpl) server.getClusterManager();
      clusterManager.clear();
      server.stop(true);


      // Wait to be informed of failure
      boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.util.TestableServer#getServer()
    */
   public HornetQServer getServer()
   {
      return server;
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

}
