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

import java.util.List;

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
   
   public void testRedist() throws Throwable
   {
      redistTest(true);
   }
   
   /*
    * 
    * 
    * 
    */
   public void redistTest(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         office4 = createClusteredPostOffice("node4", "testgroup");
         
         office5 = createClusteredPostOffice("node5", "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, "node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding1 = office1.bindClusteredQueue("queue1", queue1);
         PullingReceiver receiver1 = new PullingReceiver();
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, "node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding2 = office2.bindClusteredQueue("queue1", queue2);
         PullingReceiver receiver2 = new PullingReceiver();
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office3, "node3", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding3 = office3.bindClusteredQueue("queue1", queue3);
         PullingReceiver receiver3 = new PullingReceiver();
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office4, "node4", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding4 = office4.bindClusteredQueue("queue1", queue4);
         PullingReceiver receiver4 = new PullingReceiver();
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office5, "node5", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding5 = office5.bindClusteredQueue("queue1", queue5);
         PullingReceiver receiver5 = new PullingReceiver();
         queue5.add(receiver5);
         
         //Send 30 messages to each queue
         this.sendMessages("queue1", persistent, office1, 30, null);
         this.sendMessages("queue1", persistent, office2, 30, null);
         this.sendMessages("queue1", persistent, office3, 30, null);
         this.sendMessages("queue1", persistent, office4, 30, null);
         this.sendMessages("queue1", persistent, office5, 30, null);
         
         Thread.sleep(500);
         
         List msgs = queue1.browse();
         assertEquals(30, msgs.size());
         
         msgs = queue2.browse();
         assertEquals(30, msgs.size());
         
         msgs = queue3.browse();
         assertEquals(30, msgs.size());
         
         msgs = queue4.browse();
         assertEquals(30, msgs.size());
         
         msgs = queue5.browse();
         assertEquals(30, msgs.size());
         
         //Consume all the messages from queue 3
         for (int i = 0; i < 30; i++)
         {
            Delivery del = receiver3.getDelivery();
            log.info("Got delivery: " + del.getReference().getMessageID());
            del.acknowledge(null);
            queue3.deliver(false);
         }
         
         msgs = queue3.browse();
         assertEquals(0, msgs.size());
         
         queue3.deliver(false);
         
         Delivery del = receiver3.getDelivery();
         
         log.info("delivery is " + del);
         
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
   
   protected ClusteredPostOffice createClusteredPostOffice(String nodeId, String groupName) throws Exception
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



