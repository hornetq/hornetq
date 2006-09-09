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
package org.jboss.test.messaging.core.plugin;

import java.util.List;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.contract.Binding;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusteredPostOfficeImpl;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleReceiver;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A ClusteredPostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredPostOfficeTest extends SimplePostOfficeTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClusteredPostOfficeTest(String name)
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
   
   public final void testClusteredBindSameName() throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      try
      {         
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         Queue queue1 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 =
            office1.bindQueue("sub1", "topic1", false, queue1);
                  
         try
         {
            office2.bindQueue("sub1", "topic1", false, queue1);
            fail();
         }
         catch (IllegalArgumentException e)
         {
            //OK - this should fail
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
      }
   }
   
   
   public final void testClusteredBindUnbind() throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      try
      {         
         //Start one office
         
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         //Add a couple of bindings
         
         Queue queue1 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding1 =
            office1.bindClusteredQueue("sub1", "topic1", false, queue1);
         Queue queue2 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding2 =
            office1.bindClusteredQueue("sub2", "topic1", false, queue2);
         
         //Start another office - make sure it picks up the bindings from the first node
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         List bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));         
         
         //Add another binding on node 2
         
         Queue queue3 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding3 =
            office2.bindClusteredQueue("sub3", "topic1", false, queue3);
  
         //Make sure both nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));

         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));

         //Add another binding on node 1
         
         Queue queue4 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding4 =
            office2.bindClusteredQueue("sub4", "topic1", false, queue4);
         
         // Make sure both nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));
         assertEquivalent(binding4, (Binding)bindings.get(3));
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));
         assertEquivalent(binding4, (Binding)bindings.get(3));
         
         //Unbind binding 1 and binding 2
         office1.unbindClusteredQueue("sub1");
         office1.unbindClusteredQueue("sub2");
         
         //Make sure bindings are not longer available on either node
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
   
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         
         //Add a third office
                  
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         //Maks sure it picks up the bindings
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         
         //Add another binding on node 3
                  
         Queue queue5 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding5 =
            office3.bindClusteredQueue("sub5", "topic1", false, queue5);
         
         // Make sure all nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         //Add a durable and a non durable binding on node 1
         
         Queue queue6 = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding6 =
            office1.bindClusteredQueue("sub6", "topic1", false, queue6);
         
         Queue queue7 = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         Binding binding7 =
            office1.bindClusteredQueue("sub7", "topic1", false, queue7);
         
         // Make sure all nodes pick them up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
               
         //Stop office 1
         office1.stop();
  
         //Need to sleep since it may take some time for the view changed request to reach the
         //members which causes the bindings to be removed
         
         Thread.sleep(1000);
         
         //All it's non durable bindings should be removed from the other nodes
         //Durable bindings should remain
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         
         //Stop office 2
         office2.stop();
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         //Restart office 1 and office 2
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         //Stop all offices
         
         office1.stop();
         office2.stop();
         office3.stop();
         
         //Start them all
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         //Only the durable queue should survive
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
                  
      }
      finally
      {
         if (office1 != null)
         {
            office1.unbindClusteredQueue("sub6");
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
         
         if (office3 != null)
         {
            office2.stop();
         }
      }
      
   }
   
   public final void testClusteredRoutePersistent() throws Throwable
   {
      clusteredRoute(true);
   }
   
   public final void testClusteredRouteNonPersistent() throws Throwable
   {
      clusteredRoute(false);
   }
   
   public final void testClusteredTransactionalRoutePersistent() throws Throwable
   {
      clusteredTransactionalRoute(true);
   }
   
   public final void testClusteredTransactionalRouteNonPersistent() throws Throwable
   {
      clusteredTransactionalRoute(false);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   

   protected void clusteredRoute(boolean persistentMessage) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
      
         //Two topics with a mixture of durable and non durable subscriptions
         
         Queue[] queues = new Queue[16];
         Binding[] bindings = new Binding[16];
         
         queues[0] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[0] = office1.bindClusteredQueue("sub1", "topic1", false, queues[0]);
         
         queues[1] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[1] = office1.bindClusteredQueue("sub2", "topic1", false, queues[1]);
         
         queues[2] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[2] = office2.bindClusteredQueue("sub3", "topic1", false, queues[2]);
         
         queues[3] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[3] = office2.bindClusteredQueue("sub4", "topic1", false, queues[3]);
         
         queues[4] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[4] = office2.bindClusteredQueue("sub5", "topic1", false, queues[4]);
         
         queues[5] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[5] = office1.bindClusteredQueue("sub6", "topic1", false, queues[5]);
         
         queues[6] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[6] = office1.bindClusteredQueue("sub7", "topic1", false, queues[6]);
         
         queues[7] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[7] = office1.bindClusteredQueue("sub8", "topic1", false, queues[7]);
         
         queues[8] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[8] = office1.bindClusteredQueue("sub9", "topic2", false, queues[8]);
         
         queues[9] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[9] = office1.bindClusteredQueue("sub10", "topic2", false, queues[9]);
         
         queues[10] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[10] = office2.bindClusteredQueue("sub11", "topic2", false, queues[10]);
         
         queues[11] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[11] = office2.bindClusteredQueue("sub12", "topic2", false, queues[11]);
         
         queues[12] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[12] = office2.bindClusteredQueue("sub13", "topic2", false, queues[12]);
         
         queues[13] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[13] = office1.bindClusteredQueue("sub14", "topic2", false, queues[13]);
         
         queues[14] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[14] = office1.bindClusteredQueue("sub15", "topic2", false, queues[14]);
         
         queues[15] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[15] = office1.bindClusteredQueue("sub16", "topic2", false, queues[15]);
       
         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         Message msg = MessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref = ms.reference(msg);         

         boolean routed = office1.route(ref, "topic1", null);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {         
            log.info("is is: " + i);
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(1, msgs.size());
            Message msgRec = (Message)msgs.get(0);
            assertEquals(msg.getMessageID(), msgRec.getMessageID());
            receivers[i].acknowledge(msgRec, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
                  
         //Now route to topic2
         
         msg = MessageFactory.createCoreMessage(2, persistentMessage, null);;      
         ref = ms.reference(msg);         

         routed = office1.route(ref, "topic2", null);         
         assertTrue(routed);
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(1, msgs.size());
            Message msgRec = (Message)msgs.get(0);
            assertEquals(msg.getMessageID(), msgRec.getMessageID());
            receivers[i].acknowledge(msgRec, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }

      }
      finally
      {
         if (office1 != null)
         {
            office2.unbindClusteredQueue("sub5");
            office1.unbindClusteredQueue("sub7");
            office1.unbindClusteredQueue("sub8");
            office2.unbindClusteredQueue("sub13");
            office1.unbindClusteredQueue("sub15");
            office1.unbindClusteredQueue("sub16");
            
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   
   protected void clusteredTransactionalRoute(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      try
      {   
         //Start two offices
         
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
     
         Queue[] queues = new Queue[16];
         Binding[] bindings = new Binding[16];
         
         queues[0] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[0] = office1.bindClusteredQueue("sub1", "topic1", false, queues[0]);
         
         queues[1] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[1] = office1.bindClusteredQueue("sub2", "topic1", false, queues[1]);
         
         queues[2] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[2] = office2.bindClusteredQueue("sub3", "topic1", false, queues[2]);
         
         queues[3] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[3] = office2.bindClusteredQueue("sub4", "topic1", false, queues[3]);
         
         queues[4] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[4] = office2.bindClusteredQueue("sub5", "topic1", false, queues[4]);
         
         queues[5] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[5] = office1.bindClusteredQueue("sub6", "topic1", false, queues[5]);
         
         queues[6] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[6] = office1.bindClusteredQueue("sub7", "topic1", false, queues[6]);
         
         queues[7] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[7] = office1.bindClusteredQueue("sub8", "topic1", false, queues[7]);
         
         queues[8] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[8] = office1.bindClusteredQueue("sub9", "topic2", false, queues[8]);
         
         queues[9] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[9] = office1.bindClusteredQueue("sub10", "topic2", false, queues[9]);
         
         queues[10] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[10] = office2.bindClusteredQueue("sub11", "topic2", false, queues[10]);
         
         queues[11] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[11] = office2.bindClusteredQueue("sub12", "topic2", false, queues[11]);
         
         queues[12] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[12] = office2.bindClusteredQueue("sub13", "topic2", false, queues[12]);
         
         queues[13] = new Queue(im.getId(), ms, pm, true, false, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[13] = office1.bindClusteredQueue("sub14", "topic2", false, queues[13]);
         
         queues[14] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[14] = office1.bindClusteredQueue("sub15", "topic2", false, queues[14]);
         
         queues[15] = new Queue(im.getId(), ms, pm, true, true, 2000, 100, 100, (QueuedExecutor)pool.get(), null);         
         bindings[15] = office1.bindClusteredQueue("sub16", "topic2", false, queues[15]);

         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         //First for topic 1
         
         Message msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();

         boolean routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);

         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);            
            assertTrue(msgs.isEmpty());                        
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);         
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         //Now send some non transactionally
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", null);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);         
         
         //And acknowledge transactionally
         
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // and the rollback
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
                 
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                                 
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
                           
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // Now for topic 2
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);    
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);     
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic2", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", tx);         
         assertTrue(routed);
         
         
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         //Now send some non transactionally
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);      
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
         
         //And acknowledge transactionally
         
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].clear();
         }
         
         
         
         tx.commit();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // and the rollback
         
         msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
          
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
            
            
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
            
            
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
         }
         
         
         
         tx.rollback();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                                              
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);             
            
            receivers[i].clear();
         }
      }
      finally
      {
         if (office1 != null)
         {
            office2.unbindClusteredQueue("sub5");
            office1.unbindClusteredQueue("sub7");
            office1.unbindClusteredQueue("sub8");
            office2.unbindClusteredQueue("sub13");
            office1.unbindClusteredQueue("sub15");
            office1.unbindClusteredQueue("sub16");
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   protected ClusteredPostOffice createClusteredPostOffice(String nodeId, String groupName) throws Exception
   {
      ClusteredPostOfficeImpl postOffice = 
         new ClusteredPostOfficeImpl(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, groupName,
                                 JGroupsUtil.getControlStackProperties(50, 1),
                                 JGroupsUtil.getDataStackProperties(50, 1),
                                 tr, pm, 5000, 5000);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



