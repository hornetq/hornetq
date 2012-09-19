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

import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.tests.util.CountDownSessionFailureListener;

/**
 * A SameProcessHornetQServer
 *
 * @author jmesnil
 */
public class SameProcessHornetQServer implements TestableServer
{
   private final HornetQServer server;

   public SameProcessHornetQServer(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public boolean isActive()
   {
      return server.isActive();
   }

   public void setIdentity(String identity)
   {
      server.setIdentity(identity);
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
      crash(true, sessions);
   }

   public void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      CountDownLatch latch = new CountDownLatch(sessions.length);
      for (ClientSession session : sessions)
      {
         CountDownSessionFailureListener listener = new CountDownSessionFailureListener(latch);
         session.addFailureListener(listener);
      }

      ClusterManager clusterManager = server.getClusterManager();
      clusterManager.flushExecutor();
      clusterManager.clear();
      Assert.assertTrue("server should be running!", server.isStarted());
      server.stop(true);

      if (waitFailure)
      {
         // Wait to be informed of failure
         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         Assert.assertTrue("Failed to stop the server! Latch count is " + latch.getCount() + " out of " +
                  sessions.length, ok);
      }
   }

   public HornetQServer getServer()
   {
      return server;
   }
}
