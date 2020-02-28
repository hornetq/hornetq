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

package org.hornetq.tests.integration.cluster.topology;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * A NettyHAClientTopologyWithDiscoveryTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class NettyHAClientTopologyWithDiscoveryTest extends HAClientTopologyWithDiscoveryTest
{

   @Override
   protected boolean isNetty()
   {
      return true;
   }


   private ServerLocatorImpl cloneWithSerialization(ServerLocatorImpl serverLocator) throws Exception {
      ByteArrayOutputStream byteout = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteout);
      objectOutputStream.writeObject(serverLocator);

      ByteArrayInputStream byteIn = new ByteArrayInputStream(byteout.toByteArray());
      ObjectInputStream objectInputStream = new ObjectInputStream(byteIn);

      return (ServerLocatorImpl)objectInputStream.readObject();
   }

   @Test
   public void testRecoveryBadUDPWithRetry() throws Exception {
      testRecoveryBadUDPWithRetry(false);
   }

   @Test
   public void testRecoveryBadUDPWithRetrySerial() throws Exception {
      testRecoveryBadUDPWithRetry(true);
   }

   public void testRecoveryBadUDPWithRetry(boolean serial) throws Exception {
      startServers(0);
      ServerLocatorImpl serverLocator = (ServerLocatorImpl) createHAServerLocator();
      serverLocator.setInitialConnectAttempts(10);

      if (serial) serverLocator = cloneWithSerialization(serverLocator);

      addServerLocator(serverLocator);

      serverLocator.initialize();
      serverLocator.getDiscoveryGroup().stop();


      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession();
      session.close();
   }

   @Test
   public void testRecoveryBadUDPWithoutRetry() throws Exception {
      testRecoveryBadUDPWithoutRetry(false);
   }


   @Test
   public void testRecoveryBadUDPWithoutRetrySerial() throws Exception {
      testRecoveryBadUDPWithoutRetry(true);
   }

   void testRecoveryBadUDPWithoutRetry(boolean serial) throws Exception {
      startServers(0);
      ServerLocatorImpl serverLocator = (ServerLocatorImpl) createHAServerLocator();

      if (serial) serverLocator = cloneWithSerialization(serverLocator);
      addServerLocator(serverLocator);

      serverLocator.setInitialConnectAttempts(0);
      serverLocator.initialize();
      serverLocator.getDiscoveryGroup().stop();


      boolean failure = false;
      try {
         ClientSessionFactory factory = serverLocator.createSessionFactory();
         ClientSession session = factory.createSession();
         session.close();
         factory.close();
      } catch (Exception e) {
         e.printStackTrace();
         failure = true;
      }

      Assert.assertTrue(failure);

      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession();
      session.close();
      factory.close();

   }

   @Test
   public void testNoServer() throws Exception {
      testNoServer(false);

   }
   @Test
   public void testNoServerSerial() throws Exception {
      testNoServer(true);
   }


   public void testNoServer(boolean serial) throws Exception {

      ServerLocatorImpl  serverLocator = (ServerLocatorImpl)HornetQClient.createServerLocatorWithHA(
         new DiscoveryGroupConfiguration(HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, 100,
                                         new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1)));
      serverLocator.setInitialConnectAttempts(3);
      if (serial) serverLocator = cloneWithSerialization(serverLocator);
      addServerLocator(serverLocator);

      try {
         serverLocator.createSessionFactory();
         Assert.fail("Exception was expected");
      } catch (Exception e) {
      }
   }

   @Test
   public void testConnectWithMultiThread() throws Exception {
      testConnectWithMultiThread(false);
   }

   @Test
   public void testConnectWithMultiThreadSerial() throws Exception {
      testConnectWithMultiThread(true);
   }

   public void testConnectWithMultiThread(boolean serial) throws Exception {
      final AtomicInteger errors = new AtomicInteger(0);
      int NUMBER_OF_THREADS = 100;
      final CyclicBarrier barrier = new CyclicBarrier(NUMBER_OF_THREADS);


      ServerLocatorImpl  serverLocatorTmp = (ServerLocatorImpl)HornetQClient.createServerLocatorWithHA(
         new DiscoveryGroupConfiguration(HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, 1000,
                                         new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1)));
      if (serial) serverLocatorTmp = cloneWithSerialization(serverLocatorTmp);


      final ServerLocatorImpl serverLocator = serverLocatorTmp;

      addServerLocator(serverLocator);
      serverLocator.setInitialConnectAttempts(0);

      startServers(0);

      try {

         serverLocator.setInitialConnectAttempts(0);

         Runnable runnable = new Runnable() {
            @Override
            public void run() {
               try {
                  barrier.await();

                  ClientSessionFactory factory = serverLocator.createSessionFactory();
                  ClientSession session = factory.createSession();
                  session.close();
                  factory.close();

               } catch (Exception e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         };


         Thread[] threads = new Thread[NUMBER_OF_THREADS];

         for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(runnable);
            threads[i].start();
         }

         for (Thread t : threads) {
            t.join();
         }


         Assert.assertEquals(0, errors.get());

         serverLocator.close();

         serverLocator.getDiscoveryGroup().stop();
      } finally {
         stopServers(0);
      }
   }

}
