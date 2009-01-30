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
 * A OnewayTwoNodeClusterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 30 Jan 2009 18:03:28
 *
 *
 */
public class OnewayTwoNodeClusterTest extends ClusterTestBase
{
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      setupServer(0, false, false);
      setupServer(1, false, false);
      
      setupClusterConnection("cluster1", 0, 1, "queues", false, false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers(0, 1);
      
      super.tearDown();
   }
   
   public void testStartTargetServerBeforeSourceServer() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(1, "queues.testaddress", "queue0", null, false);
      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 0);
      verifyNotReceive(0);
   }
   
   public void testStartSourceServerBeforeTargetServer() throws Exception
   {
      startServers(0, 1);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(1, "queues.testaddress", "queue0", null, false);
      addConsumer(0, 1, "queue0", null);
          
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 0);
      verifyNotReceive(0);
   }

   public void testBasicLocalReceive() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      addConsumer(0, 0, "queue0", null);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 0);
      verifyNotReceive(0);

      addConsumer(1, 0, "queue0", null);
      verifyNotReceive(1);
   }

   public void testBasicRoundRobin() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);
   }
   
   public void testRoundRobinMultipleQueues() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      
      createQueue(0, "queues.testaddress", "queue1", null, false);
      createQueue(1, "queues.testaddress", "queue1", null, false);
      
      createQueue(0, "queues.testaddress", "queue2", null, false);
      createQueue(1, "queues.testaddress", "queue2", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      
      addConsumer(2, 0, "queue1", null);
      addConsumer(3, 1, "queue1", null);
      
      addConsumer(4, 0, "queue2", null);
      addConsumer(5, 1, "queue2", null);

      waitForBindings(0, "queues.testaddress", 3, 3, true);
      waitForBindings(0, "queues.testaddress", 3, 3, false);

      send(0, "queues.testaddress", 10, false, null);
                  
      verifyReceiveRoundRobin(10, 0, 1);
      
      verifyReceiveRoundRobin(10, 2, 3);
      
      verifyReceiveRoundRobin(10, 4, 5);
      
      verifyNotReceive(0, 1, 2, 3, 4, 5);
   }
   
   public void testMultipleNonLoadBalancedQueues() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue1", null, false);
      createQueue(0, "queues.testaddress", "queue2", null, false);
      createQueue(0, "queues.testaddress", "queue3", null, false);
      createQueue(0, "queues.testaddress", "queue4", null, false);
    
      
      createQueue(1, "queues.testaddress", "queue5", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(1, "queues.testaddress", "queue7", null, false);
      createQueue(1, "queues.testaddress", "queue8", null, false);
      createQueue(1, "queues.testaddress", "queue9", null, false);

      
      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 0, "queue1", null);
      addConsumer(2, 0, "queue2", null);
      addConsumer(3, 0, "queue3", null);
      addConsumer(4, 0, "queue4", null);
      
      addConsumer(5, 1, "queue5", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(7, 1, "queue7", null);
      addConsumer(8, 1, "queue8", null);
      addConsumer(9, 1, "queue9", null);
           
      waitForBindings(0, "queues.testaddress", 5, 5, true);
      waitForBindings(0, "queues.testaddress", 5, 5, false);

      send(0, "queues.testaddress", 10, false, null);
                  
      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
      
      verifyNotReceive(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
   }
   
   public void testMixtureLoadBalancedAndNonLoadBalancedQueues() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue1", null, false);
      createQueue(0, "queues.testaddress", "queue2", null, false);
      createQueue(0, "queues.testaddress", "queue3", null, false);
      createQueue(0, "queues.testaddress", "queue4", null, false);
    
      
      createQueue(1, "queues.testaddress", "queue5", null, false);
      createQueue(1, "queues.testaddress", "queue6", null, false);
      createQueue(1, "queues.testaddress", "queue7", null, false);
      createQueue(1, "queues.testaddress", "queue8", null, false);
      createQueue(1, "queues.testaddress", "queue9", null, false);
      
      createQueue(0, "queues.testaddress", "queue10", null, false);
      createQueue(1, "queues.testaddress", "queue10", null, false);
      
      createQueue(0, "queues.testaddress", "queue11", null, false);
      createQueue(1, "queues.testaddress", "queue11", null, false);
      
      createQueue(0, "queues.testaddress", "queue12", null, false);
      createQueue(1, "queues.testaddress", "queue12", null, false);

      
      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 0, "queue1", null);
      addConsumer(2, 0, "queue2", null);
      addConsumer(3, 0, "queue3", null);
      addConsumer(4, 0, "queue4", null);
      
      addConsumer(5, 1, "queue5", null);
      addConsumer(6, 1, "queue6", null);
      addConsumer(7, 1, "queue7", null);
      addConsumer(8, 1, "queue8", null);
      addConsumer(9, 1, "queue9", null);
      
      addConsumer(10, 0, "queue10", null);
      addConsumer(11, 1, "queue10", null);
      
      addConsumer(12, 0, "queue11", null);
      addConsumer(13, 1, "queue11", null);
      
      addConsumer(14, 0, "queue12", null);
      addConsumer(15, 1, "queue12", null);
           
      waitForBindings(0, "queues.testaddress", 8, 8, true);
      waitForBindings(0, "queues.testaddress", 8, 8, false);

      send(0, "queues.testaddress", 10, false, null);
                  
      verifyReceiveAll(10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
      
      verifyReceiveRoundRobin(10, 10, 11);
      verifyReceiveRoundRobin(10, 12, 13);
      verifyReceiveRoundRobin(10, 14, 15);
      
      verifyNotReceive(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
   }
   
   public void testNotRouteToNonMatchingAddress() throws Exception
   {
      startServers(1, 0);

      setupSessionFactory(0, false);
      setupSessionFactory(1, false);

      createQueue(0, "queues.testaddress", "queue0", null, false);                
      createQueue(1, "queues.testaddress", "queue1", null, false);
      
      createQueue(0, "queues.testaddress2", "queue2", null, false);
      createQueue(1, "queues.testaddress2", "queue2", null, false);
      createQueue(0, "queues.testaddress2", "queue3", null, false);
      createQueue(1, "queues.testaddress2", "queue4", null, false);
            
      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue1", null);
      addConsumer(2, 0, "queue2", null);
      addConsumer(3, 1, "queue2", null);
      addConsumer(4, 0, "queue3", null);
      addConsumer(5, 1, "queue4", null);
                 
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      waitForBindings(0, "queues.testaddress2", 2, 2, true);
      waitForBindings(0, "queues.testaddress2", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
                  
      verifyReceiveAll(10, 0, 1);
      
      verifyNotReceive(2, 3, 4, 5);
   }


}
