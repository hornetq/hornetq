/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultMessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.plugin.base.ClusteringTestBase;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;


public class RedistributionTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public RedistributionTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {      
      super.tearDown();
   }
   
   public void testRedistNonPersistent() throws Throwable
   {
      redistTest(false);
   }
   
   public void testRedistPersistent() throws Throwable
   {
      redistTest(true);
   }
   
   public void redistTest(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
          
      try
      {   
         office1 = createClusteredPostOffice(1, "testgroup");
         
         office2 = createClusteredPostOffice(2, "testgroup");
         
         office3 = createClusteredPostOffice(3, "testgroup");
         
         office4 = createClusteredPostOffice(4, "testgroup");
         
         office5 = createClusteredPostOffice(5, "testgroup");
         
         log.info("Started offices");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding1 = office1.bindClusteredQueue("queue1", queue1);
                  
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding2 = office2.bindClusteredQueue("queue1", queue2);
                  
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office3, 3, "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding3 = office3.bindClusteredQueue("queue1", queue3);         
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office4, 4, "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding4 = office4.bindClusteredQueue("queue1", queue4);
                  
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office5, 5, "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding5 = office5.bindClusteredQueue("queue1", queue5);
                  
         log.info("bound queues");
         
         //Send 30 messages to each queue
         this.sendMessages("queue1", persistent, office1, 30, null);
         this.sendMessages("queue1", persistent, office2, 30, null);
         this.sendMessages("queue1", persistent, office3, 30, null);
         this.sendMessages("queue1", persistent, office4, 30, null);
         this.sendMessages("queue1", persistent, office5, 30, null);
                 
         log.info("sent messages");
         
         Thread.sleep(1000);
         
         //Check the sizes
          
         assertEquals(30, queue1.memoryRefCount());
         assertEquals(0, queue1.memoryDeliveryCount());
         
         assertEquals(30, queue2.memoryRefCount());
         assertEquals(0, queue2.memoryDeliveryCount());
           
         assertEquals(30, queue3.memoryRefCount());
         assertEquals(0, queue3.memoryDeliveryCount());
         
         assertEquals(30, queue4.memoryRefCount());
         assertEquals(0, queue4.memoryDeliveryCount());
         
         assertEquals(30, queue5.memoryRefCount());
         assertEquals(0, queue5.memoryDeliveryCount());
         
         //Now we add the receivers
         //Note that we did not do this before the send.
         //If we had done so then it's likely that the automatic redistribution
         //would have moved some around and there wouldn't be 30 in each queue
         
         PullingReceiver receiver1 = new PullingReceiver();
         queue1.add(receiver1);
         
         PullingReceiver receiver2 = new PullingReceiver();
         queue2.add(receiver2);
         
         PullingReceiver receiver3 = new PullingReceiver();
         queue3.add(receiver3);
         
         PullingReceiver receiver4 = new PullingReceiver();
         queue4.add(receiver4);
         
         PullingReceiver receiver5 = new PullingReceiver();
         queue5.add(receiver5);
                 
         log.info("Added receivers");
         
         //Prompt delivery so a message pops into each receiver
         queue1.deliver(true);
         queue2.deliver(true);
         queue3.deliver(true);
         queue4.deliver(true);
         queue5.deliver(true);
         
         Thread.sleep(1000);
         
         //Now we check the sizes again in case automatic balancing has erroneously
         //kicked in                  
         
         assertEquals(29, queue1.memoryRefCount());
         assertEquals(1, queue1.memoryDeliveryCount());
         
         assertEquals(29, queue2.memoryRefCount());
         assertEquals(1, queue2.memoryDeliveryCount());
           
         assertEquals(29, queue3.memoryRefCount());
         assertEquals(1, queue3.memoryDeliveryCount());
         
         assertEquals(29, queue4.memoryRefCount());
         assertEquals(1, queue4.memoryDeliveryCount());
         
         assertEquals(29, queue5.memoryRefCount());
         assertEquals(1, queue5.memoryDeliveryCount());
         
         Thread.sleep(5000);
         
         //And again - should still be no redistribution
         
         assertEquals(29, queue1.memoryRefCount());
         assertEquals(1, queue1.memoryDeliveryCount());
         
         assertEquals(29, queue2.memoryRefCount());
         assertEquals(1, queue2.memoryDeliveryCount());
           
         assertEquals(29, queue3.memoryRefCount());
         assertEquals(1, queue3.memoryDeliveryCount());
         
         assertEquals(29, queue4.memoryRefCount());
         assertEquals(1, queue4.memoryDeliveryCount());
         
         assertEquals(29, queue5.memoryRefCount());
         assertEquals(1, queue5.memoryDeliveryCount());
         
         Thread.sleep(2000);
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.memoryDeliveryCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.memoryDeliveryCount());         
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.memoryDeliveryCount());         
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.memoryDeliveryCount());         
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.memoryDeliveryCount());
                           
         log.info("trying to consume");
         
         //So we have 150 messages in total - 30 on each node.
         
         //If redistribution works ok, we should be able to do something like the following:
         
         //Consume 10 on node 1
         
         //Consume 50 on node 2
         
         //Consume 75 on node 3
         
         //Consume 10 on node 4
         
         //We leave the last 5 since they will be as deliveries in the receivers probably
         
         Delivery del;
                  
         log.info("consuming queue1");
         for (int i = 0; i < 10; i++)
         {       
            queue1.deliver(true);
            del = receiver1.getDelivery();
            log.info("Got delivery: " + del.getReference().getMessageID());
            del.acknowledge(null);  
         }
         log.info("consumed queue1");
         
         log.info("Here are the sizes:");  
         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.memoryDeliveryCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.memoryDeliveryCount());         
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.memoryDeliveryCount());         
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.memoryDeliveryCount());         
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.memoryDeliveryCount());
                  
         log.info("consuming queue2");
         for (int i = 0; i < 50; i++)
         {       
            queue2.deliver(true);
            del = receiver2.getDelivery();
            log.info("Got delivery: " + del.getReference().getMessageID());
            del.acknowledge(null);  
         }
         log.info("consumed queue2");
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.memoryDeliveryCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.memoryDeliveryCount());         
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.memoryDeliveryCount());         
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.memoryDeliveryCount());         
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.memoryDeliveryCount());
         
         log.info("consuming queue3");
         for (int i = 0; i < 75; i++)
         {       
            queue3.deliver(true);
            del = receiver3.getDelivery();
            log.info("Got delivery: " + del.getReference().getMessageID());
            del.acknowledge(null);  
         }
         log.info("consumed queue3");
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.memoryDeliveryCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.memoryDeliveryCount());         
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.memoryDeliveryCount());         
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.memoryDeliveryCount());         
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.memoryDeliveryCount());
         
         log.info("consuming queue4");
         for (int i = 0; i < 10; i++)
         {       
            queue4.deliver(true);
            del = receiver4.getDelivery();
            log.info("Got delivery: " + del.getReference().getMessageID());
            del.acknowledge(null);  
         }
         log.info("consumed queue4");
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.memoryDeliveryCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.memoryDeliveryCount());         
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.memoryDeliveryCount());         
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.memoryDeliveryCount());         
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.memoryDeliveryCount());
         
      }
      finally
      { 
         if (office1 != null)
         {
            office1.stop();
         }
         
         if (office2 != null)
         {            
            office2.stop();
         }
         
         if (office3 != null)
         {
            office3.stop();
         }
         
         if (office4 != null)
         {            
            office4.stop();
         }
         
         if (office5 != null)
         {
            office5.stop();
         }
      }
   }
   
   class PullingReceiver implements Receiver
   {
      private Delivery del;

      public synchronized Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
      {
         if (del != null)
         {
            return null;
         }
         
         del = new SimpleDelivery(observer, reference, false);
         
         this.notify();
         
         return del;
      }
      
      public synchronized Delivery getDelivery()
      {
         while (del == null)
         {
            try
            {
               this.wait();
            }
            catch (InterruptedException e)
            {               
            }
         }
         Delivery ret = del;
         del = null;
         return ret;
      }
      
   }
   
   protected ClusteredPostOffice createClusteredPostOffice(int nodeId, String groupName) throws Exception
   {
      MessagePullPolicy pullPolicy = new DefaultMessagePullPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new DefaultRouterFactory();
      
      DefaultClusteredPostOffice postOffice = 
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 5000, 5000, pullPolicy, rf, 1, 1000);
      
      postOffice.start();      
      
      return postOffice;
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}



