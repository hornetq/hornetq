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
import org.jboss.messaging.core.plugin.exchange.ClusteredTopicExchange;
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
   
   //For the following tests create a simple receiver and acknowledge messages properly rather
   //than relying on removeAllReferences
   
   //testBindUnbind - bind and unbind on different nodes as members join and leave, make sure the correct state
   //is replicated - make sure durable bindings remain durable
   
   //non transactional route
   //create several subscriptions same durable some non durable on different nodes
   //route messages with no tx - make sure it arrives ok - make sure messages arrive in durable
   //subs even if node is shutdown and restarted
   
   //as above but transactional
   
   //test that activate etc is local only
   
   //test that can't bind twice with same name across cluster
   
   //test that can't do exchange operations on an exchange that is stopped
   
   //start and stop exchanges concurrently to test for race conditions
   
   //Need to add tests for queue/topic with selector
   
   //Need to route with peristent and non persistent messages
   
   public void testClusteredBindUnbind() throws Throwable
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
         assertEquivalent(binding4, (Binding)bindings.get(2));
         
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
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         
         //Add a third exchange
                  
         exchange1 = createExchange("node3", "testgroup");
         
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
         
         //Add a durable binding on node 1
         
         Binding binding6 = exchange1.bindQueue("sub6", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         // Make sure all nodes pick it up
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         assertEquivalent(binding3, (Binding)bindings.get(0));
         assertEquivalent(binding4, (Binding)bindings.get(1));
         assertEquivalent(binding5, (Binding)bindings.get(2));
         assertEquivalent(binding6, (Binding)bindings.get(3));
         
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
         
         //Stop exchange 1
         exchange1.stop();
         
         //All it's non durable bindings should be removed from the other nodes
         //Durable bindings should remain
         
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
         
         //Stop exchange 2
         exchange2.stop();
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding4, (Binding)bindings.get(0));
         assertEquivalent(binding5, (Binding)bindings.get(1));
         
         //Restart exchange 1 and exchange 2
         exchange1.start();
         
         exchange2.start();
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding4, (Binding)bindings.get(0));
         assertEquivalent(binding5, (Binding)bindings.get(1));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding4, (Binding)bindings.get(0));
         assertEquivalent(binding5, (Binding)bindings.get(1));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         assertEquivalent(binding4, (Binding)bindings.get(0));
         assertEquivalent(binding5, (Binding)bindings.get(1));
         
         //Stop all exchanges
         
         exchange1.stop();
         exchange2.stop();
         exchange3.stop();
         
         //Start them all
         exchange1.start();
         exchange2.start();
         exchange3.start();
         
         //Only the durable queue should survive
         
         bindings = exchange1.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         
         bindings = exchange2.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
         
         bindings = exchange3.listBindingsForWildcard("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         assertEquivalent(binding5, (Binding)bindings.get(0));
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
         
         if (exchange3 != null)
         {
            exchange2.stop();
         }
      }
      
   }
   
   public void testClusteredRoute() throws Throwable
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      Exchange exchange3 = null;
      
      try
      {   
         //Start three exchanges
         
         exchange1.start();
         exchange2.start();
         exchange3.start();
         
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
                  
         bindings[8] = exchange1.bindQueue("sub1", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[9] = exchange1.bindQueue("sub2", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         
         bindings[10] = exchange2.bindQueue("sub3", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[11] = exchange2.bindQueue("sub4", "topic2", null, false, false, ms, pm, 20000, 100, 100);         
         bindings[12] = exchange2.bindQueue("sub5", "topic2", null, false, true, ms, pm, 20000, 100, 100);      
         
         bindings[13] = exchange1.bindQueue("sub6", "topic2", null, false, false, ms, pm, 20000, 100, 100);
         bindings[14] = exchange1.bindQueue("sub7", "topic2", null, false, true, ms, pm, 20000, 100, 100);
         bindings[15] = exchange1.bindQueue("sub8", "topic2", null, false, true, ms, pm, 20000, 100, 100);
    
         MessageQueue[] queues = new MessageQueue[16];
         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            queues[i] = bindings[i].getQueue();
            receivers[i] = new SimpleReceiver(SimpleReceiver.ACCEPTING);
         }
         
         Message msg1 = MessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);         

         boolean routed = exchange1.route(ref1, "topic1", null);         
         assertTrue(routed);
         
         List msgs = receivers[0].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[0].acknowledge(msgRec, null);
         msgs = queues[0].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[1].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[1].acknowledge(msgRec, null);
         msgs = queues[1].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[2].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[2].acknowledge(msgRec, null);
         msgs = queues[2].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[3].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[3].acknowledge(msgRec, null);
         msgs = queues[3].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[4].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[4].acknowledge(msgRec, null);
         msgs = queues[4].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[5].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[5].acknowledge(msgRec, null);
         msgs = queues[5].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[6].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[6].acknowledge(msgRec, null);
         msgs = queues[6].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[7].getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertEquals(msg1.getMessageID(), msgRec.getMessageID());
         receivers[7].acknowledge(msgRec, null);
         msgs = queues[7].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = receivers[8].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[8].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[9].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[9].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[10].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[10].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[11].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[11].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[12].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[12].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[13].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[13].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[14].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[14].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receivers[15].getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         msgs = queues[15].browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         
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
         
         if (exchange3 != null)
         {
            exchange2.stop();
         }
      }
   }
   
   

   
   public void testClusteredTopic() throws Exception
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      try
      {         
         exchange1 = createExchange("node1", "testgroup");
         
         Binding binding1 = exchange1.bindQueue("sub1", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         exchange2 = createExchange("node2", "testgroup");
         
         Binding binding2 = exchange1.bindQueue("sub2", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         Binding binding3 = exchange2.bindQueue("sub3", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         Message msg1 = MessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = exchange1.route(ref1, "topic1", null); 
         
         //For non reliable messages may take little while for messages to arrive
         //Since they are just put on the bus and the caller continues - so we sleep
         Thread.sleep(500);
         
         checkQueueContainsRef(binding1, ref1);
         checkQueueContainsRef(binding2, ref1);
         checkQueueContainsRef(binding3, ref1);
         
         log.info("Done test");
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
   
   private void checkQueueContainsRef(Binding binding, MessageReference ref)
   {
      MessageQueue queue = binding.getQueue();
      
      List refs = queue.browse();
      
      assertNotNull(refs);
      
      assertEquals(1, refs.size());
      
      Message msg = (Message)refs.get(0);
      
      assertEquals(ref.getMessageID(), msg.getMessageID());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected Exchange createExchange() throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, "group1", "Topic", "node1", ms, im, pool, tr, pm);
      
      exchange.start();
      
      return exchange;
   }
   
   protected Exchange createExchange(String nodeId, String groupName) throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, groupName, "Topic", nodeId, ms, im, pool, tr, pm);
      
      exchange.start();
      
      return exchange;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



