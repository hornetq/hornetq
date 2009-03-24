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


package org.jboss.messaging.tests.integration.cluster.distribution;


/**
 * A SymmetricClusterTestWithBackup
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Mar 2009 11:00:31
 *
 *
 */
public class SymmetricClusterTestWithBackup extends SymmetricClusterTest
{
//Temporarily commented out
//   @Override   
//   public void testStopAllStartAll() throws Exception
//   {
//      setupCluster();
//
//      startServers();
//
//      setupSessionFactory(0, isNetty());
//      setupSessionFactory(1, isNetty());
//      setupSessionFactory(2, isNetty());
//      setupSessionFactory(3, isNetty());
//      setupSessionFactory(4, isNetty());
//
//      createQueue(0, "queues.testaddress", "queue0", null, false);
//      createQueue(1, "queues.testaddress", "queue0", null, false);
//      createQueue(2, "queues.testaddress", "queue0", null, false);
//      createQueue(3, "queues.testaddress", "queue0", null, false);
//      createQueue(4, "queues.testaddress", "queue0", null, false);
//
//      addConsumer(0, 0, "queue0", null);
//      addConsumer(1, 1, "queue0", null);
//      addConsumer(2, 2, "queue0", null);
//      addConsumer(3, 3, "queue0", null);
//      addConsumer(4, 4, "queue0", null);
//
//      waitForBindings(0, "queues.testaddress", 1, 1, true);
//      waitForBindings(1, "queues.testaddress", 1, 1, true);
//      waitForBindings(2, "queues.testaddress", 1, 1, true);
//      waitForBindings(3, "queues.testaddress", 1, 1, true);
//      waitForBindings(4, "queues.testaddress", 1, 1, true);
//
//      waitForBindings(0, "queues.testaddress", 4, 4, false);
//      waitForBindings(1, "queues.testaddress", 4, 4, false);
//      waitForBindings(2, "queues.testaddress", 4, 4, false);
//      waitForBindings(3, "queues.testaddress", 4, 4, false);
//      waitForBindings(4, "queues.testaddress", 4, 4, false);
//
//      send(0, "queues.testaddress", 10, false, null);
//
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2, 3, 4);
//
//      this.verifyNotReceive(0, 1, 2, 3, 4);
//
//      this.removeConsumer(0);
//      this.removeConsumer(1);
//      this.removeConsumer(2);
//      this.removeConsumer(3);
//      this.removeConsumer(4);
//
//      this.closeAllSessionFactories();
//
//      stopServers(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
//
//      startServers();
//
//      setupSessionFactory(0, isNetty());
//      setupSessionFactory(1, isNetty());
//      setupSessionFactory(2, isNetty());
//      setupSessionFactory(3, isNetty());
//      setupSessionFactory(4, isNetty());
//
//      createQueue(0, "queues.testaddress", "queue0", null, false);
//      createQueue(1, "queues.testaddress", "queue0", null, false);
//      createQueue(2, "queues.testaddress", "queue0", null, false);
//      createQueue(3, "queues.testaddress", "queue0", null, false);
//      createQueue(4, "queues.testaddress", "queue0", null, false);
//
//      addConsumer(0, 0, "queue0", null);
//      addConsumer(1, 1, "queue0", null);
//      addConsumer(2, 2, "queue0", null);
//      addConsumer(3, 3, "queue0", null);
//      addConsumer(4, 4, "queue0", null);
//
//      waitForBindings(0, "queues.testaddress", 1, 1, true);
//      waitForBindings(1, "queues.testaddress", 1, 1, true);
//      waitForBindings(2, "queues.testaddress", 1, 1, true);
//      waitForBindings(3, "queues.testaddress", 1, 1, true);
//      waitForBindings(4, "queues.testaddress", 1, 1, true);
//
//      waitForBindings(0, "queues.testaddress", 4, 4, false);
//      waitForBindings(1, "queues.testaddress", 4, 4, false);
//      waitForBindings(2, "queues.testaddress", 4, 4, false);
//      waitForBindings(3, "queues.testaddress", 4, 4, false);
//      waitForBindings(4, "queues.testaddress", 4, 4, false);
//
//      send(0, "queues.testaddress", 10, false, null);
//
//      verifyReceiveRoundRobinInSomeOrder(10, 0, 1, 2, 3, 4);
//
//      this.verifyNotReceive(0, 1, 2, 3, 4);
//   }
   
   @Override
   public void testMixtureLoadBalancedAndNonLoadBalancedQueuesAddQueuesAndConsumersBeforeAllServersAreStarted() throws Exception
   {
      setupCluster();
      
      startServers(5, 0);

      setupSessionFactory(0, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue5", null, false);
      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(0, "queues.testaddress", "queue15", null, false);
      createQueue(0, "queues.testaddress", "queue17", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(5, 0, "queue5", null);

      startServers(6, 1);
      setupSessionFactory(1, isNetty());

      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(1, "queues.testaddress", "queue11", null, false);
      createQueue(1, "queues.testaddress", "queue15", null, false);
      createQueue(1, "queues.testaddress", "queue17", null, false);

      addConsumer(1, 1, "queue1", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(11, 1, "queue11", null);
      addConsumer(16, 1, "queue15", null);

      startServers(7, 2);
      setupSessionFactory(2, isNetty());

      createQueue(2, "queues.testaddress", "queue2", null, false);
      createQueue(2, "queues.testaddress", "queue7", null, false);
      createQueue(2, "queues.testaddress", "queue12", null, false);
      createQueue(2, "queues.testaddress", "queue15", null, false);
      createQueue(2, "queues.testaddress", "queue16", null, false);

      addConsumer(2, 2, "queue2", null);
      addConsumer(7, 2, "queue7", null);
      addConsumer(12, 2, "queue12", null);
      addConsumer(17, 2, "queue15", null);

      startServers(8, 3);
      setupSessionFactory(3, isNetty());

      createQueue(3, "queues.testaddress", "queue3", null, false);
      createQueue(3, "queues.testaddress", "queue8", null, false);
      createQueue(3, "queues.testaddress", "queue13", null, false);
      createQueue(3, "queues.testaddress", "queue15", null, false);
      createQueue(3, "queues.testaddress", "queue16", null, false);
      createQueue(3, "queues.testaddress", "queue18", null, false);

      addConsumer(3, 3, "queue3", null);
      addConsumer(8, 3, "queue8", null);
      addConsumer(13, 3, "queue13", null);
      addConsumer(18, 3, "queue15", null);

      startServers(9, 4);
      setupSessionFactory(4, isNetty());

      createQueue(4, "queues.testaddress", "queue4", null, false);
      createQueue(4, "queues.testaddress", "queue9", null, false);
      createQueue(4, "queues.testaddress", "queue14", null, false);
      createQueue(4, "queues.testaddress", "queue15", null, false);
      createQueue(4, "queues.testaddress", "queue16", null, false);
      createQueue(4, "queues.testaddress", "queue17", null, false);
      createQueue(4, "queues.testaddress", "queue18", null, false);

      addConsumer(4, 4, "queue4", null);
      addConsumer(9, 4, "queue9", null);
      addConsumer(10, 0, "queue10", null);
      addConsumer(14, 4, "queue14", null);

      addConsumer(15, 0, "queue15", null);
      addConsumer(19, 4, "queue15", null);

      addConsumer(20, 2, "queue16", null);
      addConsumer(21, 3, "queue16", null);
      addConsumer(22, 4, "queue16", null);

      addConsumer(23, 0, "queue17", null);
      addConsumer(24, 1, "queue17", null);
      addConsumer(25, 4, "queue17", null);

      addConsumer(26, 3, "queue18", null);
      addConsumer(27, 4, "queue18", null);

      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(1, "queues.testaddress", 5, 5, true);
      waitForBindings(2, "queues.testaddress", 5, 5, true);
      waitForBindings(3, "queues.testaddress", 6, 6, true);
      waitForBindings(4, "queues.testaddress", 7, 7, true);

      waitForBindings(0, "queues.testaddress", 23, 23, false);
      waitForBindings(1, "queues.testaddress", 23, 23, false);
      waitForBindings(2, "queues.testaddress", 23, 23, false);
      waitForBindings(3, "queues.testaddress", 22, 22, false);
      waitForBindings(4, "queues.testaddress", 21, 21, false);

      send(0, "queues.testaddress", 10, false, null);

      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

      verifyReceiveRoundRobinInSomeOrder(10, 15, 16, 17, 18, 19);

      verifyReceiveRoundRobinInSomeOrder(10, 20, 21, 22);

      verifyReceiveRoundRobinInSomeOrder(10, 23, 24, 25);

      verifyReceiveRoundRobinInSomeOrder(10, 26, 27);
   }
   
   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      //The lives
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 1, 2, 3, 4);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 0, 2, 3, 4);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 2, 0, 1, 3, 4);

      setupClusterConnection("cluster3", "queues", forwardWhenNoConsumers, 1, isNetty(), 3, 0, 1, 2, 4);

      setupClusterConnection("cluster4", "queues", forwardWhenNoConsumers, 1, isNetty(), 4, 0, 1, 2, 3);
      
      
      //The backups
      
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 5, 1, 2, 3, 4);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 6, 0, 2, 3, 4);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 7, 0, 1, 3, 4);

      setupClusterConnection("cluster3", "queues", forwardWhenNoConsumers, 1, isNetty(), 8, 0, 1, 2, 4);

      setupClusterConnection("cluster4", "queues", forwardWhenNoConsumers, 1, isNetty(), 9, 0, 1, 2, 3);
   }

   @Override
   protected void setupServers() throws Exception
   {
      //The backups
      setupServer(5, isFileStorage(), isNetty(), true);
      setupServer(6, isFileStorage(), isNetty(), true);
      setupServer(7, isFileStorage(), isNetty(), true);
      setupServer(8, isFileStorage(), isNetty(), true);
      setupServer(9, isFileStorage(), isNetty(), true);   
      
      //The lives
      setupServer(0, isFileStorage(), isNetty(), 5);
      setupServer(1, isFileStorage(), isNetty(), 6);
      setupServer(2, isFileStorage(), isNetty(), 7);
      setupServer(3, isFileStorage(), isNetty(), 8);
      setupServer(4, isFileStorage(), isNetty(), 9); 
   }
   
   @Override
   protected void startServers() throws Exception
   {
      startServers(5, 6, 7, 8, 9, 0, 1, 2, 3, 4);
   }

   @Override
   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      // We stop the cluster connections first since this makes server shutdown quicker
      stopClusterConnections(5, 6, 7, 8, 9, 0, 1, 2, 3, 4);

      stopServers(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
   }

}
