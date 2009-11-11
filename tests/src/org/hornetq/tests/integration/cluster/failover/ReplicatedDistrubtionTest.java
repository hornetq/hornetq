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

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A SymmetricFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicatedDistrubtionTest extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("test.SomeAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRedistribution() throws Exception
   {
      setupSessionFactory(1, 0, true, true);
      setupSessionFactory(3, 2, true, true);

      ClientSession sessionOne = sfs[1].createSession(true, true);

      ClientSession sessionThree = sfs[3].createSession(false, false);

      sessionOne.createQueue(ADDRESS, ADDRESS, true);

      sessionThree.createQueue(ADDRESS, ADDRESS, true);

      ClientConsumer consThree = sessionThree.createConsumer(ADDRESS);

      sessionThree.start();

      waitForBindings(3, "test.SomeAddress", 1, 1, true);
      waitForBindings(1, "test.SomeAddress", 1, 1, false);
      try
      {
         ClientProducer producer = sessionOne.createProducer(ADDRESS);

         for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = sessionOne.createClientMessage(true);
            msg.setBody(ChannelBuffers.wrappedBuffer(new byte[1024]));
            msg.putIntProperty(new SimpleString("key"), i);
            producer.send(msg);
         }

         sessionOne.commit();

         for (int i = 0; i < 50; i++)
         {
            ClientMessage msg = consThree.receive(15000);

            assertNotNull(msg);

            System.out.println(i + " msg = " + msg);

            int received = (Integer)msg.getObjectProperty(new SimpleString("key"));

            assertEquals(i, received);

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

            assertNotNull(msg);

            System.out.println(i + " msg = " + msg);

            int received = (Integer)msg.getObjectProperty(new SimpleString("key"));

            assertEquals(i, received);

            msg.acknowledge();
         }

         assertNull(consThree.receiveImmediate());

         sessionThree.commit();

         sessionOne.start();

         ClientConsumer consOne = sessionOne.createConsumer(ADDRESS);

         assertNull(consOne.receiveImmediate());

      }
      finally
      {
         sessionOne.close();
         sessionThree.close();
      }
   }

   public void testSimpleRedistributionOverReplication() throws Exception
   {
      setupSessionFactory(1, 0, true, true);
      setupSessionFactory(3, 2, true, true);

      ClientSession sessionOne = sfs[1].createSession(true, true);

      ClientSession sessionThree = sfs[3].createSession(false, false);

      sessionOne.createQueue(ADDRESS, ADDRESS, true);

      sessionThree.createQueue(ADDRESS, ADDRESS, true);

      ClientConsumer consThree = sessionThree.createConsumer(ADDRESS);

      sessionThree.start();

      waitForBindings(3, "test.SomeAddress", 1, 1, true);

      try
      {
         ClientProducer producer = sessionOne.createProducer(ADDRESS);

         for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = sessionOne.createClientMessage(true);
            msg.setBody(ChannelBuffers.wrappedBuffer(new byte[1024]));
            msg.putIntProperty(new SimpleString("key"), i);
            producer.send(msg);
         }

         sessionOne.commit();

         for (int i = 0; i < 100; i++)
         {
            ClientMessage msg = consThree.receive(15000);

            assertNotNull(msg);

            System.out.println(i + " msg = " + msg);

            int received = (Integer)msg.getObjectProperty(new SimpleString("key"));

            if (i != received)
            {
               // Shouldn't this be a failure?
               System.out.println(i + "!=" + received);
            }
            msg.acknowledge();
         }

         sessionThree.commit();

         sessionOne.start();

         ClientConsumer consOne = sessionOne.createConsumer(ADDRESS);

         assertNull(consOne.receiveImmediate());

      }
      finally
      {
         sessionOne.close();
         sessionThree.close();
      }
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

      class MyListener implements SessionFailureListener
      {
         public void connectionFailed(final HornetQException me)
         {
            latch.countDown();
         }

         /* (non-Javadoc)
          * @see org.hornetq.core.client.SessionFailureListener#beforeReconnect(org.hornetq.core.exception.HornetQException)
          */
         public void beforeReconnect(final HornetQException exception)
         {
         }
      }

      session.addFailureListener(new MyListener());

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      // Wait to be informed of failure

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServer(0, true, isShared(), true, true, -1);
      setupServer(1, true, isShared(), true, false, 0);
      setupServer(2, true, isShared(), true, true, -1);
      setupServer(3, true, isShared(), true, true,  2);

      setupClusterConnectionWithBackups("test", "test", false, 1, true, 1, new int[] { 3 }, new int[] { 2 });

      AddressSettings as = new AddressSettings();
      as.setRedistributionDelay(0);

      getServer(0).getAddressSettingsRepository().addMatch("test.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("test.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("test.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("test.*", as);

      servers[0].start();
      servers[2].start();
      servers[1].start();
      servers[3].start();
   }

   protected boolean isShared()
   {
      return false;
   }

   @Override
   protected void tearDown() throws Exception
   {
      servers[2].stop();
      servers[0].stop();
      servers[1].stop();
      servers[3].stop();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
