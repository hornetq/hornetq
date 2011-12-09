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

package org.hornetq.tests.integration.cluster.distribution;

import junit.framework.Assert;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.logging.Logger;

/**
 * A TemporaryQueueClusterTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class TemporaryQueueClusterTest extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   private static final Logger log = Logger.getLogger(ClusteredRequestResponseTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServers();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers();

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   /**
    * https://jira.jboss.org/jira/browse/HORNETQ-286
    * 
    * the test checks that the temp queue is properly propagated to the cluster
    * (assuming we wait for the bindings)
    */
   public void testSendToTempQueueFromAnotherClusterNode() throws Exception
   {
      setupCluster();

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      String tempAddress = "queues.tempaddress";
      String tempQueue = "tempqueue";
      // create temp queue on node #0
      ClientSession session =  sfs[0].createSession(false, true, true);
      session.createTemporaryQueue(tempAddress, tempQueue);
      ClientConsumer consumer = session.createConsumer(tempQueue);

      // check the binding is created on node #1
      waitForBindings(1, tempAddress, 1, 1, false);

      // send to the temp address on node #1
      send(1, tempAddress, 10, false, null);

      session.start();

      // check consumer bound to node #0 receives from the temp queue
      for (int j = 0; j < 10; j++)
      {
         ClientMessage message = consumer.receive(5000);
         if (message == null)
         {
            Assert.assertNotNull("consumer did not receive message on temp queue " + j, message);
         }
         message.acknowledge();
      }
      
      consumer.close();
      session.deleteQueue(tempQueue);
      session.close();
   }

   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 0);
   }

   protected void setupServers() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
   }

   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
