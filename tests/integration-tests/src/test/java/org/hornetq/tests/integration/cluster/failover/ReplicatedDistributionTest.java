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

package org.hornetq.tests.integration.cluster.failover;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.CountDownSessionFailureListener;

/**
 * A SymmetricFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicatedDistributionTest extends ClusterTestBase
{
   // Constants -----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("test.SomeAddress");
   private ClientSession sessionOne;
   private ClientSession sessionThree;
   private ClientConsumer consThree;
   private ClientProducer producer;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRedistribution() throws Exception
   {
      commonTestCode();


         for (int i = 0; i < 50; i++)
         {
            ClientMessage msg = consThree.receive(15000);

            Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

            int received = msg.getIntProperty("key");

            Assert.assertEquals(i, received);

            msg.acknowledge();
         }

         sessionThree.commit();

         // consThree.close();

         // TODO: Remove this sleep: If a node fail,
         // Redistribution may loose messages between the nodes.
         Thread.sleep(500);

         fail(sessionThree);

         // sessionThree.close();
         //
         // setupSessionFactory(2, -1, true);
         //
         // sessionThree = sfs[2].createSession(true, true);
         //
         // sessionThree.start();

         // consThree = sessionThree.createConsumer(ADDRESS);

         for (int i = 50; i < 100; i++)
         {
            ClientMessage msg = consThree.receive(15000);

            Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

            int received = (Integer)msg.getObjectProperty(new SimpleString("key"));

            Assert.assertEquals(i, received);

            msg.acknowledge();
         }

         Assert.assertNull(consThree.receiveImmediate());

         sessionThree.commit();

         sessionOne.start();

         ClientConsumer consOne = sessionOne.createConsumer(ReplicatedDistributionTest.ADDRESS);

         Assert.assertNull(consOne.receiveImmediate());
   }

   public void testSimpleRedistribution() throws Exception
   {
      commonTestCode();

      for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = consThree.receive(15000);

            Assert.assertNotNull(msg);

         // System.out.println(i + " msg = " + msg);

            int received = msg.getIntProperty("key");

            if (i != received)
            {
               // Shouldn't this be a failure?
               System.out.println(i + "!=" + received);
            }
            msg.acknowledge();
         }

         sessionThree.commit();

         sessionOne.start();

         ClientConsumer consOne = sessionOne.createConsumer(ReplicatedDistributionTest.ADDRESS);

         Assert.assertNull(consOne.receiveImmediate());
   }

   private void commonTestCode() throws Exception, HornetQException
   {
      waitForBindings(3, "test.SomeAddress", 1, 1, true);
      waitForBindings(1, "test.SomeAddress", 1, 1, false);

      producer = sessionOne.createProducer(ReplicatedDistributionTest.ADDRESS);

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = sessionOne.createMessage(true);
         msg.putIntProperty(new SimpleString("key"), i);
         producer.send(msg);
      }
      sessionOne.commit();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   /**
    * @param session
    * @param latch
    * @throws InterruptedException
    */
   private void fail(final ClientSession session) throws InterruptedException
   {

      final CountDownLatch latch = new CountDownLatch(1);

      session.addFailureListener(new CountDownSessionFailureListener(latch));

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupLiveServer(1, true, isShared(), true);
      setupLiveServer(3, true, isShared(), true);
      setupBackupServer(2, 3, true, isShared(), true);

      final String address = ReplicatedDistributionTest.ADDRESS.toString();
      // notice the abuse of the method call, '3' is not a backup for '1'
      setupClusterConnectionWithBackups("test", address, false, 1, true, 1, new int[] { 3 });
      setupClusterConnectionWithBackups("test", address, false, 1, true, 3, new int[] { 2, 1 });

      AddressSettings as = new AddressSettings();
      as.setRedistributionDelay(0);

      for (int i : new int[] { 1, 2, 3 })
      {
         getServer(i).getAddressSettingsRepository().addMatch("test.*", as);
         getServer(i).start();
      }

      setupSessionFactory(1, -1, true, true);
      setupSessionFactory(3, 2, true, true);

      sessionOne = sfs[1].createSession(true, true);
      sessionThree = sfs[3].createSession(false, false);

      sessionOne.createQueue(ReplicatedDistributionTest.ADDRESS, ReplicatedDistributionTest.ADDRESS, true);
      sessionThree.createQueue(ReplicatedDistributionTest.ADDRESS, ReplicatedDistributionTest.ADDRESS, true);

      consThree = sessionThree.createConsumer(ReplicatedDistributionTest.ADDRESS);

      sessionThree.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         if (consThree != null)
            consThree.close();
         if (producer != null)
            producer.close();
         if (sessionOne != null)
            sessionOne.close();
         if (sessionThree != null)
            sessionThree.close();
      }
      finally
      {
         super.tearDown();
      }
   }

   protected boolean isShared()
   {
      return false;
   }
}
