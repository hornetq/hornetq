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
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.contract.Exchange;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.plugin.exchange.cluster.ClusteredTopicExchange;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jgroups.Channel;
import org.jgroups.JChannel;

/**
 * 
 * A ClusteredTopicExchangeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredTopicExchangeTest extends TopicExchangeTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClusteredTopicExchangeTest(String name)
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
   
   public final void testBindSameName() throws Throwable
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      try
      {         
         exchange1 = createExchange("node1", "testgroup");
         
         exchange2 = createExchange("node2", "testgroup");
         
         exchange1.bindQueue("sub1", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         try
         {
            exchange2.bindQueue("sub1", "topic1", null, false, false, ms, pm, 20000, 100, 100);
            fail();
         }
         catch (IllegalArgumentException e)
         {
            //OK - this should fail
         }
      }
      finally
      {
         if (exchange1 != null)
         {
            exchange1.stop();
         }
         
         if (exchange2 != null)
         {
            exchange2.stop();
         }
      }
   }
   
   
   public final void testClusteredBindUnbind() throws Throwable
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      Exchange exchange3 = null;
      
      try
      {         
         //Start one exchange
         
         exchange1 = createExchange("node1", "testgroup");
         
         //Add a couple of bindings
         
         Binding binding1 = exchange1.bindQueue("sub1", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         Binding binding2 = exchange1.bindQueue("sub2", "topic1", null, false, false, ms, pm, 20000, 100, 100);
    
         //Start another exchange - make sure it picks up the bindings from the first node
         
         exchange2 = createExchange("node2", "testgroup");
         
         List bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));         
         
         //Add another binding on node 2
         
         Binding binding3 = exchange2.bindQueue("sub3", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         //Make sure both nodes pick it up
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));

         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));

         //Add another binding on node 1
         
         Binding binding4 = exchange2.bindQueue("sub4", "topic1", null, false, false, ms, pm, 20000, 100, 100);
            
         // Make sure both nodes pick it up
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));
         assertEquivalent(binding4, (Binding)bindings.get(3));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding1, (Binding)bindings.get(0));
         assertEquivalent(binding2, (Binding)bindings.get(1));
         assertEquivalent(binding3, (Binding)bindings.get(2));
         assertEquivalent(binding4, (Binding)bindings.get(3));
         
         //Unbind binding 1 and binding 2
         exchange1.unbindQueue("sub1");
         exchange1.unbindQueue("sub2");
         
         //Make sure bindings are not longer available on either node
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
   
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         
         //Add a third exchange
                  
         exchange3 = createExchange("node3", "testgroup");
         
         //Maks sure it picks up the bindings
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         
         //Add another binding on node 3
                  
         Binding binding5 = exchange3.bindQueue("sub5", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         // Make sure all nodes pick it up
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         
         //Add a durable and a non durable binding on node 1
         
         Binding binding6 = exchange1.bindQueue("sub6", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         Binding binding7 = exchange1.bindQueue("sub7", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         // Make sure all nodes pick them up
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         assertEquivalent(binding7, (Binding)bindings.get(4));
               
         //Stop exchange 1
         exchange1.stop();
  
         //Need to sleep since it may take some time for the view changed request to reach the
         //members which causes the bindings to be removed
         
         Thread.sleep(1000);
         
         //All it's non durable bindings should be removed from the other nodes
         //Durable bindings should remain
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         
         //Stop exchange 2
         exchange2.stop();
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         //Restart exchange 1 and exchange 2
         exchange1 = createExchange("node1", "testgroup");
         
         exchange2 = createExchange("node2", "testgroup");
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         assertEquivalent(binding6, (Binding)bindings.get(1));
         
         //Stop all exchanges
         
         exchange1.stop();
         exchange2.stop();
         exchange3.stop();
         
         //Start them all
         exchange1 = createExchange("node1", "testgroup");
         exchange2 = createExchange("node2", "testgroup");
         exchange3 = createExchange("node3", "testgroup");
         
         //Only the durable queue should survive
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding6, (Binding)bindings.get(0));
                  
      }
      finally
      {
         if (exchange1 != null)
         {
            exchange1.reloadQueues("topic1", ms, pm, 1000, 20, 20);
            exchange1.unbindQueue("sub6");
            exchange1.stop();
         }
         
         if (exchange2 != null)
         {
            exchange2.stop();
         }
         
         if (exchange3 != null)
         {
            exchange2.stop();
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
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
          
      try
      {   
         //Start two exchanges
         
         exchange1 = createExchange("node1", "testgroup");
         exchange2 = createExchange("node2", "testgroup");
      
         //Two topics with a mixture of durable and non durable subscriptions
         
         Binding[] bindings = new Binding[16];
         
         bindings[0] = exchange1.bindQueue("sub1", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[1] = exchange1.bindQueue("sub2", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         bindings[2] = exchange2.bindQueue("sub3", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[3] = exchange2.bindQueue("sub4", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[4] = exchange2.bindQueue("sub5", "topic1", null, false, true, ms, pm, 20000, 100, 100);      
         
         bindings[5] = exchange1.bindQueue("sub6", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         bindings[6] = exchange1.bindQueue("sub7", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         bindings[7] = exchange1.bindQueue("sub8", "topic1", null, false, true, ms, pm, 20000, 100, 100);
                  
         bindings[8] = exchange1.bindQueue("sub9", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[9] = exchange1.bindQueue("sub10", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         
         bindings[10] = exchange2.bindQueue("sub11", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[11] = exchange2.bindQueue("sub12", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[12] = exchange2.bindQueue("sub13", "topic2", null, false, true, ms, pm, 20000, 100, 100);      
         
         bindings[13] = exchange1.bindQueue("sub14", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         bindings[14] = exchange1.bindQueue("sub15", "topic2", null, false, true, ms, pm, 20000, 100, 100);
         bindings[15] = exchange1.bindQueue("sub16", "topic2", null, false, true, ms, pm, 20000, 100, 100);
    
         MessageQueue[] queues = new MessageQueue[16];
         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            queues[i] = bindings[i].getQueue();
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         Message msg = MessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref = ms.reference(msg);         

         boolean routed = exchange1.route(ref, "topic1", null);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
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

         routed = exchange1.route(ref, "topic2", null);         
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
         if (exchange1 != null)
         {
            exchange2.unbindQueue("sub5");
            exchange1.unbindQueue("sub7");
            exchange1.unbindQueue("sub8");
            exchange2.unbindQueue("sub13");
            exchange1.unbindQueue("sub15");
            exchange1.unbindQueue("sub16");
            
            exchange1.stop();
         }
         
         if (exchange2 != null)
         {
            exchange2.stop();
         }
      }
   }
   
   
   protected void clusteredTransactionalRoute(boolean persistent) throws Throwable
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      try
      {   
         //Start two exchanges
         
         exchange1 = createExchange("node1", "testgroup");
         exchange2 = createExchange("node2", "testgroup");

         
         //Two topics with a mixture of durable and non durable subscriptions
         
         Binding[] bindings = new Binding[16];
         
         bindings[0] = exchange1.bindQueue("sub1", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[1] = exchange1.bindQueue("sub2", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         
         bindings[2] = exchange2.bindQueue("sub3", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[3] = exchange2.bindQueue("sub4", "topic1", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[4] = exchange2.bindQueue("sub5", "topic1", null, false, true, ms, pm, 20000, 100, 100);      
         
         bindings[5] = exchange1.bindQueue("sub6", "topic1", null, false, false, ms, pm, 20000, 100, 100);
         bindings[6] = exchange1.bindQueue("sub7", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         bindings[7] = exchange1.bindQueue("sub8", "topic1", null, false, true, ms, pm, 20000, 100, 100);
                  
         bindings[8] = exchange1.bindQueue("sub9", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[9] = exchange1.bindQueue("sub10", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         
         bindings[10] = exchange2.bindQueue("sub11", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[11] = exchange2.bindQueue("sub12", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[12] = exchange2.bindQueue("sub13", "topic2", null, false, true, ms, pm, 20000, 100, 100);      
         
         bindings[13] = exchange1.bindQueue("sub14", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         bindings[14] = exchange1.bindQueue("sub15", "topic2", null, false, true, ms, pm, 20000, 100, 100);
         bindings[15] = exchange1.bindQueue("sub16", "topic2", null, false, true, ms, pm, 20000, 100, 100);
    
         MessageQueue[] queues = new MessageQueue[16];
         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            queues[i] = bindings[i].getQueue();
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         //First for topic 1
         
         Message msg1 = MessageFactory.createCoreMessage(1, persistent, null);;      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = MessageFactory.createCoreMessage(2, persistent, null);;      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();

         boolean routed = exchange1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic1", tx);         
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

         routed = exchange1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic1", tx);         
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
         
         routed = exchange1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic1", null);         
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
         
         routed = exchange1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic1", null);         
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

         routed = exchange1.route(ref1, "topic2", tx);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic2", tx);         
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

         routed = exchange1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic1", tx);         
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
         
         routed = exchange1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic2", null);         
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
         
         routed = exchange1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = exchange1.route(ref2, "topic2", null);         
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
         if (exchange1 != null)
         {
            exchange2.unbindQueue("sub5");
            exchange1.unbindQueue("sub7");
            exchange1.unbindQueue("sub8");
            exchange2.unbindQueue("sub13");
            exchange1.unbindQueue("sub15");
            exchange1.unbindQueue("sub16");
            exchange1.stop();
         }
         
         if (exchange2 != null)
         {
            exchange2.stop();
         }
      }
   }
   
   protected Exchange createExchange() throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      exchange.start();
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, "group1", "Topic", "node1", ms, im, pool, tr, pm);
                  
      return exchange;
   }
   
   protected Exchange createExchange(String nodeId, String groupName) throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      
      exchange.start();      
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, groupName, "Topic", nodeId, ms, im, pool, tr, pm);
            
      return exchange;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



