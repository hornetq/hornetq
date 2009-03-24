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

import org.jboss.messaging.core.logging.Logger;

/**
 * A OneWayChainClusterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 7 Feb 2009 15:23:08
 *
 *
 */
public class OneWayChainClusterTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(OneWayChainClusterTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());  
      setupServer(2, isFileStorage(), isNetty());
      setupServer(3, isFileStorage(), isNetty());  
      setupServer(4, isFileStorage(), isNetty());
   }

   @Override
   protected void tearDown() throws Exception
   {
      closeAllConsumers();
      
      closeAllSessionFactories();
      
      stopClusterConnections(0, 1, 2, 3, 4);
      
      stopServers(0, 1, 2, 3, 4);
      
      super.tearDown();
   }
   
   protected boolean isNetty()
   {
      return false;
   }
   
   protected boolean isFileStorage()
   {
      return false;
   }
   
   public void testBasicRoundRobin() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);            
   }
   
   public void testBasicNonLoadBalanced() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(0, "queues.testaddress", "queue1", null, false);

      createQueue(4, "queues.testaddress", "queue2", null, false);
      createQueue(4, "queues.testaddress", "queue3", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 0, "queue1", null);

      addConsumer(2, 4, "queue2", null);
      addConsumer(3, 4, "queue3", null);

      waitForBindings(0, "queues.testaddress", 2, 2, true);
      waitForBindings(0, "queues.testaddress", 2, 2, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 0, 1, 2, 3);
      verifyNotReceive(0, 1, 2, 3);       
   }
   
   public void testRoundRobinForwardWhenNoConsumersTrue() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", true, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", true, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", true, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", true, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);
      
      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);            
   }
   
   public void testRoundRobinForwardWhenNoConsumersFalseNoLocalQueue() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());
    
      createQueue(4, "queues.testaddress", "queue0", null, false);
     
      waitForBindings(0, "queues.testaddress", 1, 0, false);
      
      addConsumer(1, 4, "queue0", null);
     
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveAll(10, 1);
      verifyNotReceive(1);            
   }
   
   public void testRoundRobinForwardWhenNoConsumersFalse() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);
      
      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);
 
      //Should still be round robin'd since there's no local consumer
      
      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);            
   }
   
   public void testRoundRobinForwardWhenNoConsumersFalseLocalConsumer() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);
      
      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);
            
      send(0, "queues.testaddress", 10, false, null);
      
      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      verifyReceiveAll(10, 0);
      verifyNotReceive(0, 1);            
   }
      
   public void testHopsTooLow() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 3, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 3, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 3, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 3, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      send(0, "queues.testaddress", 10, false, null);  
      
      verifyReceiveAll(10, 0);
      
      verifyNotReceive(1);
   }
   
   public void testStartStopMiddleOfChain() throws Exception
   {
      setupClusterConnection("cluster0-1", 0, 1, "queues", false, 4, isNetty());
      setupClusterConnection("cluster1-2", 1, 2, "queues", false, 4, isNetty());
      setupClusterConnection("cluster2-3", 2, 3, "queues", false, 4, isNetty());
      setupClusterConnection("cluster3-4", 3, 4, "queues", false, 4, isNetty());
            
      startServers(0, 1, 2, 3, 4);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(4, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(4, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 4, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1); 
      
      stopServers(2);
      
      startServers(2);

      Thread.sleep(2000);
        
      send(0, "queues.testaddress", 10, false, null);
       
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1); 
   }
   
}
