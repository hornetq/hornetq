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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.ConditionFactory;
import org.jboss.messaging.core.plugin.contract.FailoverMapper;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultFailoverMapper;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultMessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleCondition;
import org.jboss.test.messaging.core.SimpleConditionFactory;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.PostOfficeTestBase;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A RedistributionWithDefaultMessagePullPolicyTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class RedistributionWithDefaultMessagePullPolicyTest extends PostOfficeTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public RedistributionWithDefaultMessagePullPolicyTest(String name)
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
   
   public void testConsumeAllNonPersistentNonRecoverable() throws Throwable
   {
      consumeAll(false, false);
   }
   
   public void testConsumeAllPersistentNonRecoverable() throws Throwable
   {
      consumeAll(true, false);
   }
   
   public void testConsumeAllNonPersistentRecoverable() throws Throwable
   {
      consumeAll(false, true);
   }
   
   public void testConsumeAllPersistentRecoverable() throws Throwable
   {
      consumeAll(true, true);
   }
         
   public void testConsumeBitByBitNonPersistentNonRecoverable() throws Throwable
   {
      consumeBitByBit(false, false);
   }
   
   public void testConsumeBitByBitPersistentNonRecoverable() throws Throwable
   {
      consumeBitByBit(true, false);
   }
   
   public void testConsumeBitByBitNonPersistentRecoverable() throws Throwable
   {
      consumeBitByBit(false, true);
   }
   
   public void testConsumeBitByBitPersistentRecoverable() throws Throwable
   {
      consumeBitByBit(true, true);
   }
   
   public void testSimpleMessagePull() throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 =
            office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 =
            office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                          
         Message msg = CoreMessageFactory.createCoreMessage(1);   
         msg.setReliable(true);
         
         MessageReference ref = ms.reference(msg);  
         
         office1.route(ref, new SimpleCondition("queue1"), null);
                  
         Thread.sleep(2000);
         
         //Messages should all be in queue1
         
         List msgs = queue1.browse();
         assertEquals(1, msgs.size());
         
         msgs = queue2.browse();
         assertTrue(msgs.isEmpty());
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver1.setMaxRefs(0);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver2.setMaxRefs(0);
         queue2.add(receiver2);
         
         //Prompt delivery so the channels know if the receivers are ready
         queue1.deliver(false);
         Thread.sleep(2000);
           
         //Pull from 1 to 2
         
         receiver2.setMaxRefs(1);
         
         log.info("delivering");
         queue2.deliver(false);                 
         
         Thread.sleep(3000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());         
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         log.info("r2 " + receiver2.getMessages().size());
         
         log.info("queue1 refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2 refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(1, queue2.getDeliveringCount());
         
         this.acknowledgeAll(receiver2);
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
         
         assertTrue(office1.getHoldingTransactions().isEmpty());         
         assertTrue(office2.getHoldingTransactions().isEmpty());
           
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
      }
   }
   
   public void testSimpleMessagePullCrashBeforeCommit() throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 =
            office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 =
            office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                          
         Message msg = CoreMessageFactory.createCoreMessage(1);   
         msg.setReliable(true);
         
         MessageReference ref = ms.reference(msg);  
         
         office1.route(ref, new SimpleCondition("queue1"), null);
                  
         Thread.sleep(2000);
         
         //Messages should all be in queue1
         
         List msgs = queue1.browse();
         assertEquals(1, msgs.size());
         
         msgs = queue2.browse();
         assertTrue(msgs.isEmpty());
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver1.setMaxRefs(0);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver2.setMaxRefs(0);
         queue2.add(receiver2);
         
         //Prompt delivery so the channels know if the receivers are ready
         queue1.deliver(false);
         Thread.sleep(2000);
           
         //Pull from 1 to 2
         
         receiver2.setMaxRefs(1);
         
         //Force a failure before commit
         office2.setFail(true, false, false);
         
         log.info("delivering");
         queue2.deliver(false);                 
         
         Thread.sleep(3000);
         
         assertEquals(1, office1.getHoldingTransactions().size());         
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         log.info("queue1 refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2 refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(1, queue1.getDeliveringCount());
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
         
         //Now kill office 2 - this should cause office1 to remove the dead held transaction
         
         office2.stop();         
         Thread.sleep(2000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());        
 
         //The delivery should be cancelled back to the queue too
         
         assertEquals(1, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
            
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
      }
   }
   
   public void testSimpleMessagePullCrashAfterCommit() throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 =
            office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 =
            office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                          
         Message msg = CoreMessageFactory.createCoreMessage(1);   
         msg.setReliable(true);
         
         MessageReference ref = ms.reference(msg);  
         
         office1.route(ref, new SimpleCondition("queue1"), null);
                  
         Thread.sleep(2000);
         
         //Messages should all be in queue1
         
         List msgs = queue1.browse();
         assertEquals(1, msgs.size());
         
         msgs = queue2.browse();
         assertTrue(msgs.isEmpty());
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver1.setMaxRefs(0);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver2.setMaxRefs(0);
         queue2.add(receiver2);
         
         //Prompt delivery so the channels know if the receivers are ready
         queue1.deliver(false);
         Thread.sleep(2000);
           
         //Pull from 1 to 2
         
         receiver2.setMaxRefs(1);
         
         //Force a failure after commit the ack to storage
         office2.setFail(false, true, false);
         
         log.info("delivering");
         queue2.deliver(false);                 
         
         Thread.sleep(3000);
         
         assertEquals(1, office1.getHoldingTransactions().size());         
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         log.info("queue1 refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2 refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(1, queue1.getDeliveringCount());
            
         //Now kill office 2 - this should cause office1 to remove the dead held transaction
         
         office2.stop();         
         Thread.sleep(2000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());        
         
         //The delivery should be committed
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
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
      }
   }
   
   public void testFailHandleMessagePullResult() throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 =
            office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 =
            office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                          
         Message msg = CoreMessageFactory.createCoreMessage(1);   
         msg.setReliable(true);
         
         MessageReference ref = ms.reference(msg);  
         
         office1.route(ref, new SimpleCondition("queue1"), null);
                  
         Thread.sleep(2000);
         
         //Messages should all be in queue1
         
         List msgs = queue1.browse();
         assertEquals(1, msgs.size());
         
         msgs = queue2.browse();
         assertTrue(msgs.isEmpty());
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver1.setMaxRefs(0);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);
         receiver2.setMaxRefs(0);
         queue2.add(receiver2);
         
         //Prompt delivery so the channels know if the receivers are ready
         queue1.deliver(false);
         Thread.sleep(2000);
           
         //Pull from 1 to 2
         
         receiver2.setMaxRefs(1);
         
         office2.setFail(false, false, true);
         
         log.info("delivering");
         queue2.deliver(false);                 
         
         Thread.sleep(3000);
         
         //The delivery should be rolled back
         
         assertTrue(office2.getHoldingTransactions().isEmpty());        
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         log.info("queue1 refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2 refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         
         assertEquals(1, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
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
      }
   }
   
   protected void consumeAll(boolean persistent, boolean recoverable) throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      DefaultClusteredPostOffice office3 = null;
      
      DefaultClusteredPostOffice office4 = null;
      
      DefaultClusteredPostOffice office5 = null;
          
      try
      {   
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         office3 = (DefaultClusteredPostOffice)createClusteredPostOffice(3, "testgroup");
         
         office4 = (DefaultClusteredPostOffice)createClusteredPostOffice(4, "testgroup");
         
         office5 = (DefaultClusteredPostOffice)createClusteredPostOffice(5, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 = office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
                  
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 = office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                  
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office3, 3, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding3 = office3.bindClusteredQueue(new SimpleCondition("queue1"), queue3);         
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office4, 4, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding4 = office4.bindClusteredQueue(new SimpleCondition("queue1"), queue4);
                  
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office5, 5, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding5 = office5.bindClusteredQueue(new SimpleCondition("queue1"), queue5);
                   
         final int NUM_MESSAGES = 100;
         
         this.sendMessages("queue1", persistent, office1, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office2, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office3, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office4, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office5, NUM_MESSAGES, null);
                 
         Thread.sleep(2000);
         
         //Check the sizes
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
               
         assertEquals(NUM_MESSAGES, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
           
         assertEquals(NUM_MESSAGES, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue5.memoryRefCount());
         assertEquals(0, queue5.getDeliveringCount());
         
         SimpleReceiver receiver = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         
         queue1.add(receiver);
         
         queue1.deliver(false);
         
         Thread.sleep(7000);
         
         log.info("Here are the sizes:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(NUM_MESSAGES * 5, queue1.getDeliveringCount());
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
           
         assertEquals(0, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(0, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(0, queue5.memoryRefCount());
         assertEquals(0, queue5.getDeliveringCount());
         
         List messages = receiver.getMessages();
         
         assertNotNull(messages);
         
         assertEquals(NUM_MESSAGES * 5, messages.size());
         
         Iterator iter = messages.iterator();
         
         while (iter.hasNext())
         {
            Message msg = (Message)iter.next();
            
            receiver.acknowledge(msg, null);
         }
         
         receiver.clear();
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
         
         if (checkNoMessageData())
         {
            fail("Message data still in database");
         }
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
   
   protected void consumeBitByBit(boolean persistent, boolean recoverable) throws Throwable
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      DefaultClusteredPostOffice office3 = null;
      
      DefaultClusteredPostOffice office4 = null;
      
      DefaultClusteredPostOffice office5 = null;
          
      try
      {   
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         office3 = (DefaultClusteredPostOffice)createClusteredPostOffice(3, "testgroup");
         
         office4 = (DefaultClusteredPostOffice)createClusteredPostOffice(4, "testgroup");
         
         office5 = (DefaultClusteredPostOffice)createClusteredPostOffice(5, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding1 = office1.bindClusteredQueue(new SimpleCondition("queue1"), queue1);
                  
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding2 = office2.bindClusteredQueue(new SimpleCondition("queue1"), queue2);
                  
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office3, 3, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding3 = office3.bindClusteredQueue(new SimpleCondition("queue1"), queue3);         
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office4, 4, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding4 = office4.bindClusteredQueue(new SimpleCondition("queue1"), queue4);
                  
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office5, 5, "queue1", channelIDManager.getID(), ms, pm, true, recoverable, (QueuedExecutor)pool.get(), null, tr);
         Binding binding5 = office5.bindClusteredQueue(new SimpleCondition("queue1"), queue5);
                  
         final int NUM_MESSAGES = 100;
          
         this.sendMessages("queue1", persistent, office1, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office2, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office3, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office4, NUM_MESSAGES, null);
         this.sendMessages("queue1", persistent, office5, NUM_MESSAGES, null);
                          
         Thread.sleep(2000);
                
         //Check the sizes
         
         log.info("Here are the sizes 1:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
               
         assertEquals(NUM_MESSAGES, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
           
         assertEquals(NUM_MESSAGES, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES, queue5.memoryRefCount());
         assertEquals(0, queue5.getDeliveringCount());
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
                 
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);         
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);         
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);         
         queue3.add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);         
         queue4.add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING_TO_MAX);         
         queue5.add(receiver5);
         
         receiver1.setMaxRefs(5);         
         queue1.deliver(false);         
         Thread.sleep(1000);         
         assertEquals(NUM_MESSAGES - 5, queue1.memoryRefCount());
         assertEquals(5, queue1.getDeliveringCount());
         
         acknowledgeAll(receiver1);
         assertEquals(0, queue1.getDeliveringCount());
         receiver1.setMaxRefs(0);
         
         receiver2.setMaxRefs(10);         
         queue2.deliver(false);         
         Thread.sleep(1000);
         assertEquals(NUM_MESSAGES - 10, queue2.memoryRefCount());
         assertEquals(10, queue2.getDeliveringCount());
         acknowledgeAll(receiver2);
         receiver2.setMaxRefs(0);
                  
         receiver3.setMaxRefs(15);         
         queue3.deliver(false);         
         Thread.sleep(1000);
         assertEquals(NUM_MESSAGES - 15, queue3.memoryRefCount());
         assertEquals(15, queue3.getDeliveringCount());
         acknowledgeAll(receiver3);
         receiver3.setMaxRefs(0);
         
         receiver4.setMaxRefs(20);         
         queue4.deliver(false);         
         Thread.sleep(1000);
         assertEquals(NUM_MESSAGES - 20, queue4.memoryRefCount());
         assertEquals(20, queue4.getDeliveringCount());
         acknowledgeAll(receiver4);
         receiver4.setMaxRefs(0);
         
         receiver5.setMaxRefs(25);         
         queue5.deliver(false);         
         Thread.sleep(1000);
         assertEquals(NUM_MESSAGES - 25, queue5.memoryRefCount());
         assertEquals(25, queue5.getDeliveringCount());
         acknowledgeAll(receiver5);
         receiver5.setMaxRefs(0);
         
         Thread.sleep(1000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
         
         log.info("Here are the sizes 2:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
     
         //Consume the rest from queue 5
         receiver5.setMaxRefs(NUM_MESSAGES - 25);
         queue5.deliver(false);
         Thread.sleep(5000);         
         
         log.info("receiver5 msgs:" + receiver5.getMessages().size());
         
         log.info("Here are the sizes 3:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
         
         //This will result in an extra one being pulled from queue1 - we cannot avoid this
         //This is because the channel does not know that the receiver is full unless it tries
         //with a ref so it needs to retrieve one
     
         assertEquals(NUM_MESSAGES - 6, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 10, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
          
         assertEquals(NUM_MESSAGES - 15, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 20, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(1, queue5.memoryRefCount());         
         assertEquals(NUM_MESSAGES - 25, queue5.getDeliveringCount());
         
         acknowledgeAll(receiver5);
         
         assertEquals(0, queue5.getDeliveringCount());
         
         receiver5.setMaxRefs(0);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
         
         //Now consume 5 more from queue5, they should come from queue1 which has the most messages
         
         log.info("Consume 5 more from queue 5");
         
         receiver5.setMaxRefs(5);
         queue5.deliver(false);
         Thread.sleep(5000);
           
         log.info("Here are the sizes 4:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 11, queue1.memoryRefCount());
          
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 10, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
          
         assertEquals(NUM_MESSAGES - 15, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 20, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(1, queue5.memoryRefCount());          
         assertEquals(5, queue5.getDeliveringCount());
               
         acknowledgeAll(receiver5);

         assertEquals(0, queue5.getDeliveringCount());
         
         receiver1.setMaxRefs(0);
           
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
          
         //Consume 1 more - should pull one from queue2
         
         receiver5.setMaxRefs(1);
         queue5.deliver(false);
         Thread.sleep(2000);
          
         log.info("Here are the sizes 5:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 11, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 11, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
          
         assertEquals(NUM_MESSAGES - 15, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(NUM_MESSAGES - 20, queue4.memoryRefCount());
         assertEquals(0, queue4.getDeliveringCount());
         
         assertEquals(1, queue5.memoryRefCount());          
         assertEquals(1, queue5.getDeliveringCount());
                  
         acknowledgeAll(receiver5);
         
         assertEquals(0, queue5.getDeliveringCount());
         
         receiver5.setMaxRefs(0);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
         
         //From queue 4 consume everything else
         
         receiver4.setMaxRefs(NUM_MESSAGES - 15 + NUM_MESSAGES - 20 + NUM_MESSAGES - 11 + NUM_MESSAGES - 11 + 1);
         queue4.deliver(false);
         Thread.sleep(7000);
         
         log.info("Here are the sizes 6:");         
         log.info("queue1, refs:" + queue1.memoryRefCount() + " dels:" + queue1.getDeliveringCount());
         log.info("queue2, refs:" + queue2.memoryRefCount() + " dels:" + queue2.getDeliveringCount());
         log.info("queue3, refs:" + queue3.memoryRefCount() + " dels:" + queue3.getDeliveringCount());
         log.info("queue4, refs:" + queue4.memoryRefCount() + " dels:" + queue4.getDeliveringCount());
         log.info("queue5, refs:" + queue5.memoryRefCount() + " dels:" + queue5.getDeliveringCount());
         
         assertEquals(0, queue1.memoryRefCount());
         assertEquals(0, queue1.getDeliveringCount());
         
         assertEquals(0, queue2.memoryRefCount());
         assertEquals(0, queue2.getDeliveringCount());
          
         assertEquals(0, queue3.memoryRefCount());
         assertEquals(0, queue3.getDeliveringCount());
         
         assertEquals(0, queue4.memoryRefCount());
         assertEquals(NUM_MESSAGES - 15 + NUM_MESSAGES - 20 + NUM_MESSAGES - 11 + NUM_MESSAGES - 11 + 1, queue4.getDeliveringCount());
         
         assertEquals(0, queue5.memoryRefCount());          
         assertEquals(0, queue5.getDeliveringCount());
                  
         acknowledgeAll(receiver4);
         
         assertEquals(0, queue4.getDeliveringCount());
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         assertTrue(office2.getHoldingTransactions().isEmpty());
         assertTrue(office3.getHoldingTransactions().isEmpty());
         assertTrue(office4.getHoldingTransactions().isEmpty());
         assertTrue(office5.getHoldingTransactions().isEmpty());
         
         if (checkNoMessageData())
         {
            fail("Message data still in database");
         }
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
   
   class ThrottleReceiver implements Receiver, Runnable
   {
      long pause;
      
      volatile int totalCount;
      
      int count;
      
      int maxSize;
      
      volatile boolean full;
      
      Executor executor;
      
      List dels;
      
      Channel queue;
      
      int getTotalCount()
      {
         return totalCount;
      }
      
      ThrottleReceiver(Channel queue, long pause, int maxSize)
      {
         this.queue = queue;
         
         this.pause = pause;
         
         this.maxSize = maxSize;
         
         this.executor = new QueuedExecutor();
         
         this.dels = new ArrayList();
      }

      public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
      {
         if (full)
         {
            return null;
         }
         
         //log.info(this + " got ref");
         
         //log.info("cnt:" + totalCount);
         
         SimpleDelivery del = new SimpleDelivery(observer, reference);
         
         dels.add(del);
         
         count++;
         
         totalCount++;
         
         if (count == maxSize)
         {
            full = true;
            
            count = 0;
            
            try
            {
               executor.execute(this);
            }
            catch (InterruptedException e)
            {
               //Ignore
            }
         }
         
         return del;
          
      }
      
      public void run()
      {
         //Simulate processing of messages
         
         try
         {
            Thread.sleep(pause);
         }
         catch (InterruptedException e)
         {
            //Ignore
         }
         
         Iterator iter = dels.iterator();
         
         while (iter.hasNext())
         {
            Delivery del = (Delivery)iter.next();
            
            try
            {
               del.acknowledge(null);
            }
            catch (Throwable t)
            {
               //Ignore
            }
         }
         
         dels.clear();
         
         full = false;
         
         queue.deliver(false);
      }
      
   }
   
   private void acknowledgeAll(SimpleReceiver receiver) throws Throwable
   {
      List messages = receiver.getMessages();
      
      Iterator iter = messages.iterator();
      
      while (iter.hasNext())
      {
         Message msg = (Message)iter.next();
         
         receiver.acknowledge(msg, null);
      }
      
      receiver.clear();
   }
   
      
   protected ClusteredPostOffice createClusteredPostOffice(int nodeId, String groupName) throws Exception
   {
      MessagePullPolicy pullPolicy = new DefaultMessagePullPolicy();
       
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new DefaultRouterFactory();
      
      FailoverMapper mapper = new DefaultFailoverMapper();
      
      ConditionFactory cf = new SimpleConditionFactory();           
      
      DefaultClusteredPostOffice postOffice = 
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 sc.getClusteredPostOfficeSQLProperties(), true, nodeId,
                                 "Clustered", ms, pm, tr, ff, cf, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 10000, 10000, pullPolicy, rf, mapper, 1000);
      
      postOffice.start();      
      
      return postOffice;
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}




