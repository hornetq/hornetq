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

import java.util.List;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.logging.Logger;

/**
 * A SimpleSymmetricClusterTest
 *
 * @author clebert
 *
 *
 */
public class SimpleSymmetricClusterTest extends ClusterTestBase
{

   // Constants -----------------------------------------------------

   static final Logger log = Logger.getLogger(SimpleSymmetricClusterTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   /**
    * @param name
    * @param address
    * @param forwardWhenNoConsumers
    * @param maxHops
    * @param connectorFrom
    * @param pairs
    * @return
    */
   protected ClusterConnectionConfiguration createClusterConfig(final String name,
                                                                final String address,
                                                                final boolean forwardWhenNoConsumers,
                                                                final int maxHops,
                                                                TransportConfiguration connectorFrom,
                                                                List<String> pairs)
   {
      ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration(name,
                                                                                      address,
                                                                                      connectorFrom.getName(),
                                                                                      2000,
                                                                                      true,
                                                                                      forwardWhenNoConsumers,
                                                                                      maxHops,
                                                                                      1024,
                                                                                      pairs,
                                                                                      false);
      return clusterConf;
   }

   public void tearDown() throws Exception
   {
      log.info("#test tearDown " + loopNumber);
      stopServers(0, 1, 2, 3, 4);
      super.tearDown();
   }

   public boolean isNetty()
   {
      return false;
   }

   public void testSimpleWithBackup() throws Exception
   {
      // The backups
      setupBackupServer(0, 3, isFileStorage(), true, isNetty());
      setupBackupServer(1, 4, isFileStorage(), true, isNetty());
      setupBackupServer(2, 5, isFileStorage(), true, isNetty());

      // The lives
      setupLiveServer(3, isFileStorage(), true, isNetty());
      setupLiveServer(4, isFileStorage(), true, isNetty());
      setupLiveServer(5, isFileStorage(), true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 3, 4, 5);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 4, 3, 5);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 5, 3, 4);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 4, 5);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 3, 5);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 3, 4);


      startServers(0, 1, 2, 3, 4, 5);

      log.info("");
      for (int i = 0; i <= 5; i++)
      {
         log.info(servers[i].describe());
         log.info(debugBindings(servers[i], servers[i].getConfiguration().getManagementNotificationAddress().toString()));
      }
      log.info("");
 
      log.info("");
      for (int i = 0; i <= 5; i++)
      {
         log.info(servers[i].describe());
         log.info(debugBindings(servers[i], servers[i].getConfiguration().getManagementNotificationAddress().toString()));
      }
      log.info("");
      

      
      stopServers(0, 1, 2, 3, 4, 5);

   }
   
   
   public void testSimple() throws Exception
   {
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);
      
      waitForTopology(servers[0], 3);
      waitForTopology(servers[1], 3);
      waitForTopology(servers[2], 3);
      
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

   }
   
   public void testSimple_TwoNodes() throws Exception
   {
      setupServer(0, false, isNetty());
      setupServer(1, false, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      
      closeAllConsumers();

   }

   static int loopNumber;
   public void _testLoop() throws Throwable
   {
      for (int i = 0 ; i < 10; i++)
      {
         loopNumber = i;
         log.info("#test " + i);
         testSimple();
         tearDown();
         setUp();
      }
   }


   

   public void testSimple2() throws Exception
   {
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());
      setupServer(3, true, isNetty());
      setupServer(4, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2, 3, 4);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2, 3, 4);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1, 3, 4);

      setupClusterConnection("cluster3", "queues", false, 1, isNetty(), 3, 0, 1, 2, 4);

      setupClusterConnection("cluster4", "queues", false, 1, isNetty(), 4, 0, 1, 2, 3);

      startServers(0, 1, 2, 3, 4);
      
      for (int i = 0 ; i <= 4; i++)
      {
         waitForTopology(servers[i], 5);
      }
      
      log.info("All the servers have been started already!");

      for (int i = 0; i <= 4; i++)
      {
         setupSessionFactory(i, isNetty());
      }

      for (int i = 0 ; i <= 4; i++)
      {
         createQueue(i, "queues.testaddress", "queue0", null, false);
      }

      for (int i = 0 ; i <= 4; i++)
      {
         addConsumer(i, i, "queue0", null);
      }

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 4, 4, false);
      waitForBindings(1, "queues.testaddress", 4, 4, false);
      waitForBindings(2, "queues.testaddress", 4, 4, false);

   }
   
   public void testSimpleRoundRobbin() throws Exception
   {
      
      //TODO make this test to crash a node
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, 10, 100, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, 10, 100, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster1", "queues", false, 1, 10, 100, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      // Need to wait some time so the bridges and 
      // connectors had time to connect properly between the nodes

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);
      
      stopServers(2);


      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);


      send(0, "queues.testaddress", 100, true, null);

      verifyReceiveRoundRobin(100, 0, 1);
      
      sfs[2] = null;
      consumers[2] = null;
      
      
      startServers(2);
      
      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);

      
      

   }

   
   public void _testSimpleRoundRobbinNoFailure() throws Exception
   {
      //TODO make this test to crash a node
      setupServer(0, true, isNetty());
      setupServer(1, true, isNetty());
      setupServer(2, true, isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, -1, 1000, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", false, 1, -1, 1000, isNetty(), 1, 2, 0);
      setupClusterConnection("cluster1", "queues", false, 1, -1, 1000, isNetty(), 2, 0, 1);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 33, true, null);

      verifyReceiveRoundRobin(33, 0, 1, 2);
      
      stopServers(2);


      send(0, "queues.testaddress", 100, true, null);
      
      verifyReceiveRoundRobin(100, 0, 1, -1);
      
      sfs[2] = null;
      consumers[2] = null;
      
      startServers(2);
      
      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      verifyReceiveRoundRobin(100, -1, -1, 2);

      
      

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
