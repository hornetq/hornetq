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
package org.jboss.test.messaging.core.postoffice;

import java.util.Collection;
import java.util.List;

import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.test.messaging.core.PostOfficeTestBase;
import org.jboss.test.messaging.core.SimpleCondition;
import org.jboss.test.messaging.core.SimpleFilter;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * 
 * A PostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2386 $</tt>
 *
 * $Id: DefaultPostOfficeTest.java 2386 2007-02-21 18:07:44Z timfox $
 *
 */
public class PostOfficeTest extends PostOfficeTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public PostOfficeTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public final void testBindUnbind() throws Throwable
   {
      PostOffice office1 = null;
      
      PostOffice office2 = null;
      
      PostOffice office3 = null;
      
      try
      {             
         office1 = createNonClusteredPostOffice();
         
         //Bind one durable
             
         MessagingQueue queue1 =
            new MessagingQueue(1, "durableQueue", channelIDManager.getID(), ms, pm, true, -1, null, false);
         queue1.activate();
         
         Condition condition1 = new SimpleCondition("condition1");
                  
         boolean added = office1.addBinding(new Binding(condition1, queue1, false), false);
         assertTrue(added);
         
         //Binding twice with the same name should fail      
         added = office1.addBinding(new Binding(condition1, queue1, false), false);
         assertFalse(added);
         
         //Can't bind a queue from another node
         
         try
         {
         
	         MessagingQueue queuexx =
	            new MessagingQueue(777, "durableQueue", channelIDManager.getID(), ms, pm, true, -1, null, false);
	         queuexx.activate();
	         office1.addBinding(new Binding(condition1, queuexx, false), false);
            fail();
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
               
         //Bind one non durable
         MessagingQueue queue2 =
            new MessagingQueue(1, "nonDurableQueue", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue2.activate();
         
         Condition condition2 = new SimpleCondition("condition2");         
         
         added = office1.addBinding(new Binding(condition2, queue2, false), false);
         assertTrue(added);
         
         //Check they're there
         
         Collection queues = office1.getQueuesForCondition(condition1, true);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         Queue rqueue1 = (Queue)queues.iterator().next();
         assertEquals(queue1, rqueue1);
         
         queues = office1.getQueuesForCondition(condition2, true);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         Queue rqueue2 = (Queue)queues.iterator().next();
         assertEquals(queue2, rqueue2);
                          
         office1.stop();
         
         //Throw away the office and create another
         office2 = createNonClusteredPostOffice();
         
         //Only one binding should be there
         queues = office2.getQueuesForCondition(condition1, true);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         rqueue1 = (Queue)queues.iterator().next();
         assertEquals(queue1, rqueue1);
         
         queues = office2.getQueuesForCondition(condition2, true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         //Unbind the binding
         office2.removeBinding(queue1.getName(), false);         
         
         //Make sure no longer there
         queues = office2.getQueuesForCondition(condition1, true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         queues = office2.getQueuesForCondition(condition2, true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         office2.stop();
         
         //Throw away office and start another
         office3 = createNonClusteredPostOffice();
         
         //Make sure not there
         queues = office3.getQueuesForCondition(condition1, true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         queues = office3.getQueuesForCondition(condition2, true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
                 
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
            office2.stop();
         }         
      }
            
   }
   
   public final void testGetQueuesForCondition() throws Throwable
   {
      PostOffice office = null;
      
      try
      {      
         office = createNonClusteredPostOffice();
         
         Condition condition1 = new SimpleCondition("condition1");
         
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue1.activate();
         
         office.addBinding(new Binding(condition1, queue1, false), false);
         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue2.activate();
         
         office.addBinding(new Binding(condition1, queue2, false), false);
         
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue3.activate();
         
         office.addBinding(new Binding(condition1, queue3, false), false);
         
         MessagingQueue queue4 = new MessagingQueue(1, "queue4", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue4.activate();
         
         office.addBinding(new Binding(condition1, queue4, false), false);
         
         MessagingQueue queue5 = new MessagingQueue(1, "queue5", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue5.activate();
         
         Condition condition2 = new SimpleCondition("condition2");         
         
         office.addBinding(new Binding(condition2, queue5, false), false);
         
         MessagingQueue queue6 = new MessagingQueue(1, "queue6", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue6.activate();
         
         office.addBinding(new Binding(condition2, queue6, false), false);
         
         MessagingQueue queue7 = new MessagingQueue(1, "queue7", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue7.activate();
         
         office.addBinding(new Binding(condition2, queue7, false), false);
         
         MessagingQueue queue8 = new MessagingQueue(1, "queue8", channelIDManager.getID(), ms, pm,  false, -1, null, false);
         queue8.activate();
         
         office.addBinding(new Binding(condition2, queue8, false), false);
                  
         Collection queues = office.getQueuesForCondition(new SimpleCondition("dummy"), true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
                           
         //We don't match on substrings
         queues = office.getQueuesForCondition(new SimpleCondition("condition123"), true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         //We don't currently support hierarchies
         queues = office.getQueuesForCondition(new SimpleCondition("condition1.subcondition"), true);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         //Lookup the queues
         
         queues = office.getQueuesForCondition(condition1, true);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         queues = office.getQueuesForCondition(condition2, true);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         assertTrue(queues.contains(queue7));
         assertTrue(queues.contains(queue8));
         
         //Lookup should work on new instance too
         
         queues = office.getQueuesForCondition(new SimpleCondition("condition1"), true);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         queues = office.getQueuesForCondition(new SimpleCondition("condition2"), true);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         assertTrue(queues.contains(queue7));
         assertTrue(queues.contains(queue8));
         
      }
      finally
      {
         if (office != null)
         {
            office.stop();
         }
        
      }
         
   }
   
   public void testGetBindingForQueueName() throws Throwable
   {   	
      PostOffice office = null;
      
      try
      {      
         office = createNonClusteredPostOffice();
                  
         Condition condition1 = new SimpleCondition("condition1");  
         
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition1, queue1, false), false);
                         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition1, queue2, false), false);
         
         Condition condition2 = new SimpleCondition("condition2");  
                  
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition2, queue3, false), false);
         
         Binding b1 = office.getBindingForQueueName("queue1");
         assertNotNull(b1);
         assertEquals(queue1, b1.queue);
         assertEquals(condition1, b1.condition);
         
         Binding b2 = office.getBindingForQueueName("queue2");
         assertNotNull(b2);
         assertEquals(queue2, b2.queue);
         assertEquals(condition1, b2.condition);
         
         Binding b3 = office.getBindingForQueueName("queue3");
         assertNotNull(b3);
         assertEquals(queue3, b3.queue);
         assertEquals(condition2, b3.condition);
         
         office.removeBinding("queue1", false);
         
         b1 = office.getBindingForQueueName("queue1");
         assertNull(b1);

         b2 = office.getBindingForQueueName("queue2");
         assertNotNull(b2);
         assertEquals(queue2, b2.queue);
         assertEquals(condition1, b2.condition);
         
         b3 = office.getBindingForQueueName("queue3");
         assertNotNull(b3);
         assertEquals(queue3, b3.queue);
         assertEquals(condition2, b3.condition);
         
         office.removeBinding("queue2", false);
         office.removeBinding("queue3", false);
         
         b1 = office.getBindingForQueueName("queue1");
         assertNull(b1);

         b2 = office.getBindingForQueueName("queue2");
         assertNull(b2);
 
         b3 = office.getBindingForQueueName("queue3");
         assertNull(b3);                     
                  
      }
      finally
      {
         if (office != null)
         {
            office.stop();
         }
      }         
   }
   
   public void testGetBindingForChannelID() throws Throwable
   {   	
      PostOffice office = null;
      
      try
      {      
         office = createNonClusteredPostOffice();
                  
         Condition condition1 = new SimpleCondition("condition1");  
         
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition1, queue1, false), false);
                         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition1, queue2, false), false);
         
         Condition condition2 = new SimpleCondition("condition2");  
                  
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         office.addBinding(new Binding(condition2, queue3, false), false);
         
         Binding b1 = office.getBindingForChannelID(queue1.getChannelID());
         assertNotNull(b1);
         assertEquals(queue1, b1.queue);
         assertEquals(condition1, b1.condition);
         
         Binding b2 = office.getBindingForChannelID(queue2.getChannelID());
         assertNotNull(b2);
         assertEquals(queue2, b2.queue);
         assertEquals(condition1, b2.condition);
         
         Binding b3 = office.getBindingForChannelID(queue3.getChannelID());
         assertNotNull(b3);
         assertEquals(queue3, b3.queue);
         assertEquals(condition2, b3.condition);
         
         office.removeBinding("queue1", false);
         
         b1 = office.getBindingForChannelID(queue1.getChannelID());
         assertNull(b1);

         b2 =office.getBindingForChannelID(queue2.getChannelID());
         assertNotNull(b2);
         assertEquals(queue2, b2.queue);
         assertEquals(condition1, b2.condition);
         
         b3 = office.getBindingForChannelID(queue3.getChannelID());
         assertNotNull(b3);
         assertEquals(queue3, b3.queue);
         assertEquals(condition2, b3.condition);
         
         office.removeBinding("queue2", false);
         office.removeBinding("queue3", false);
         
         b1 = office.getBindingForChannelID(queue1.getChannelID());
         assertNull(b1);

         b2 = office.getBindingForChannelID(queue2.getChannelID());
         assertNull(b2);
 
         b3 = office.getBindingForChannelID(queue3.getChannelID());
         assertNull(b3);                     

      }
      finally
      {
         if (office != null)
         {
            office.stop();
         }
        
      }
         
   }

   public void testRouteNonPersistentWithFilter() throws Throwable
   {
      routeWithFilter(false);
   }
   
   public void testRoutePersistentWithFilter() throws Throwable
   {
      routeWithFilter(true);
   }
   
   public final void testRoutePersistent() throws Throwable
   {
      route(true);
   }
   
   public final void testRouteNonPersistent() throws Throwable
   {
      route(false);
   }
   
   public final void testRouteTransactionalPersistent() throws Throwable
   {
      routeTransactional(true);
   }
   
   public final void testRouteTransactionalNonPersistent() throws Throwable
   {
      routeTransactional(false);
   }
         
   public final void testRouteInactive() throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createNonClusteredPostOffice();
         
         Condition condition1 = new SimpleCondition("topic1");
         
         MessagingQueue queue1 =  new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue1.activate();
         
         postOffice.addBinding(new Binding(condition1, queue1, false), false);
         
         MessagingQueue queue2 =  new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue2.activate();
         
         postOffice.addBinding(new Binding(condition1, queue2, false), false);
         
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue3.activate();
         
         postOffice.addBinding(new Binding(condition1, queue3, false), false);
         
         MessagingQueue queue4 = new MessagingQueue(1, "queue4", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue4.activate();
         
         Condition condition2 = new SimpleCondition("topic2");
         
         
         postOffice.addBinding(new Binding(condition2, queue4, false), false);
         
         MessagingQueue queue5 = new MessagingQueue(1, "queue5", channelIDManager.getID(), ms, pm, false,-1, null, false);
         queue5.activate();
         
         postOffice.addBinding(new Binding(condition2, queue5, false), false);
         
         MessagingQueue queue6 = new MessagingQueue(1, "queue6", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue6.activate();
         
         postOffice.addBinding(new Binding(condition2, queue6, false), false);
      
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.getLocalDistributor().add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.getLocalDistributor().add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.getLocalDistributor().add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.getLocalDistributor().add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.getLocalDistributor().add(receiver5);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue6.getLocalDistributor().add(receiver6);
         
         queue1.deactivate();
         queue2.deactivate();
         queue5.deactivate();
         queue6.deactivate();
         
         assertFalse(queue1.isActive());      
         assertFalse(queue2.isActive());
         assertFalse(queue5.isActive());
         assertFalse(queue6.isActive()); 
         assertTrue(queue3.isActive());
         assertTrue(queue4.isActive());      
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = postOffice.route(ref1, condition1, null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver3.acknowledge(msgRec, null);
         msgs = queue3.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver4.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver5.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver6.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         receiver3.clear();
                     
         Message msg2 = CoreMessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = postOffice.route(ref2, condition2, null);      
         assertTrue(routed);
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());      
         
         msgs = receiver4.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver4.acknowledge(msgRec, null);
         msgs = queue4.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver5.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver6.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
      }
   
   }

   public final void testRouteNoQueue() throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createNonClusteredPostOffice();
         
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue1.activate();
         
         postOffice.addBinding(new Binding(new SimpleCondition("condition1"), queue1, false), false);
              
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         
         queue1.getLocalDistributor().add(receiver1);
   
         assertTrue(queue1.isActive());
   
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = postOffice.route(ref1, new SimpleCondition("this won't match anything"), null);
         
         assertFalse(routed);
               
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
      }
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
      
   protected void setUp() throws Exception
   {
      super.setUp();

   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   private void route(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createNonClusteredPostOffice();
      
         Condition condition1 = new SimpleCondition("topic1");
         
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue1.activate();
         
         postOffice.addBinding(new Binding(condition1, queue1, false), false);
         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue2.activate();
         
         postOffice.addBinding(new Binding(condition1, queue2, false), false);
         
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue3.activate();
         
         postOffice.addBinding(new Binding(condition1, queue3, false), false);
         
         MessagingQueue queue4 = new MessagingQueue(1, "queue4", channelIDManager.getID(), ms, pm, true, -1, null, false);
         queue4.activate();
         
         Condition condition2 = new SimpleCondition("topic2");
         
         postOffice.addBinding(new Binding(condition2, queue4, false), false);
         
         MessagingQueue queue5 = new MessagingQueue(1, "queue5", channelIDManager.getID(), ms, pm, true, -1, null, false);
         queue5.activate();
         
         postOffice.addBinding(new Binding(condition2, queue5, false), false);
         
         MessagingQueue queue6 = new MessagingQueue(1, "queue6", channelIDManager.getID(), ms, pm, true, -1, null, false);
         queue6.activate();
         
         postOffice.addBinding(new Binding(condition2, queue6, false), false);
      
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.getLocalDistributor().add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.getLocalDistributor().add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue3.getLocalDistributor().add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue4.getLocalDistributor().add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue5.getLocalDistributor().add(receiver5);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue6.getLocalDistributor().add(receiver6);
         
         assertTrue(queue1.isActive());      
         assertTrue(queue2.isActive());
         assertTrue(queue3.isActive());      
         assertTrue(queue4.isActive());
         assertTrue(queue5.isActive());      
         assertTrue(queue6.isActive());
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = postOffice.route(ref1, condition1, null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver1.acknowledge(msgRec, null);
         msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver2.acknowledge(msgRec, null);
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver3.acknowledge(msgRec, null);
         msgs = queue3.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver4.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver5.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver6.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         receiver1.clear();
         receiver2.clear();
         receiver3.clear();
         
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = postOffice.route(ref2, condition2, null);      
         assertTrue(routed);
         
         msgs = receiver4.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver4.acknowledge(msgRec, null);
         msgs = queue4.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver5.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver5.acknowledge(msgRec, null);
         msgs = queue5.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver6.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver6.acknowledge(msgRec, null);
         msgs = queue6.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());    
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         postOffice.removeBinding("queue1", false);
         postOffice.removeBinding("queue2", false);
         postOffice.removeBinding("queue3", false);
         postOffice.removeBinding("queue4", false);
         postOffice.removeBinding("queue5", false);
         postOffice.removeBinding("queue6", false);         
         
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }         
      }
   }
   
   private void routeTransactional(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createNonClusteredPostOffice();
         
         Condition condition1 = new SimpleCondition("topic1");
      
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue1.activate();
         
         postOffice.addBinding(new Binding(condition1, queue1, false), false);
         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, true,-1, null, false);
         queue2.activate();
         
         postOffice.addBinding(new Binding(condition1, queue2, false), false);
          
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.getLocalDistributor().add(receiver1);

         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.getLocalDistributor().add(receiver2);
   
         assertTrue(queue1.isActive());
         assertTrue(queue2.isActive());
   
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();
         
         boolean routed = postOffice.route(ref1, condition1, tx);            
         assertTrue(routed);
         routed = postOffice.route(ref2, condition1, tx);            
         assertTrue(routed);
               
         List msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         tx.commit();
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         Message msgRec1 = (Message)msgs.get(0);
         Message msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg1);
         assertTrue(msgRec2 == msg2);
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg1);
         assertTrue(msgRec2 == msg2);
         
         //Acknowledge
         receiver1.acknowledge(msgRec1, null);
         receiver1.acknowledge(msgRec2, null);
         
         receiver2.acknowledge(msgRec1, null);
         receiver2.acknowledge(msgRec2, null);
   
         msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         receiver1.clear();
         
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         receiver2.clear();
              
         Message msg3 = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);      
         MessageReference ref3 = ms.reference(msg3);
         
         Message msg4 = CoreMessageFactory.createCoreMessage(4, persistentMessage, null);      
         MessageReference ref4 = ms.reference(msg4);
         
         tx = tr.createTransaction();
         
         routed = postOffice.route(ref3, condition1, tx);            
         assertTrue(routed);
         routed = postOffice.route(ref4, condition1, tx);            
         assertTrue(routed);
               
         msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         tx.rollback();
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         receiver1.clear();
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         receiver2.clear();
         
         
         Message msg5 = CoreMessageFactory.createCoreMessage(5, persistentMessage, null);      
         MessageReference ref5 = ms.reference(msg5);
         
         Message msg6 = CoreMessageFactory.createCoreMessage(6, persistentMessage, null);      
         MessageReference ref6 = ms.reference(msg6);
               
         routed = postOffice.route(ref5, new SimpleCondition("topic1"), null);            
         assertTrue(routed);
         routed = postOffice.route(ref6, new SimpleCondition("topic1"), null);            
         assertTrue(routed);
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg5);
         assertTrue(msgRec2 == msg6);
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg5);
         assertTrue(msgRec2 == msg6);
         
         //Acknowledge transactionally
         
         tx = tr.createTransaction();
         
         receiver1.acknowledge(msgRec1, tx);
         receiver1.acknowledge(msgRec2, tx);
         
         receiver2.acknowledge(msgRec1, tx);
         receiver2.acknowledge(msgRec2, tx);
         
         int deliveringCount = queue1.getDeliveringCount();
         assertEquals(2, deliveringCount);
         
         deliveringCount = queue2.getDeliveringCount();
         assertEquals(2, deliveringCount);
         
         tx.commit();
         
         msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
               
         receiver1.clear();
         
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
               
         receiver2.clear();
         
         Message msg7 = CoreMessageFactory.createCoreMessage(7, persistentMessage, null);      
         MessageReference ref7 = ms.reference(msg7);
         
         Message msg8 = CoreMessageFactory.createCoreMessage(8, persistentMessage, null);      
         MessageReference ref8 = ms.reference(msg8);
               
         routed = postOffice.route(ref7, new SimpleCondition("topic1"), null);            
         assertTrue(routed);
         routed = postOffice.route(ref8, new SimpleCondition("topic1"), null);            
         assertTrue(routed);
         
         msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);
         
         //Acknowledge transactionally
         
         tx = tr.createTransaction();
         
         receiver1.acknowledge(msgRec1, tx);
         receiver1.acknowledge(msgRec2, tx);
         
         receiver2.acknowledge(msgRec1, tx);
         receiver2.acknowledge(msgRec2, tx);
         
         deliveringCount = queue1.getDeliveringCount();
         assertEquals(2, deliveringCount);
         
         deliveringCount = queue2.getDeliveringCount();
         assertEquals(2, deliveringCount);
         
         tx.rollback();
         
         deliveringCount = queue1.getDeliveringCount();
         assertEquals(2, deliveringCount);                  
         
         receiver1.acknowledge(msgRec1, null);
         receiver1.acknowledge(msgRec2, null);
         
         deliveringCount = queue2.getDeliveringCount();
         assertEquals(2, deliveringCount);     
         
         
         receiver2.acknowledge(msgRec1, null);
         receiver2.acknowledge(msgRec2, null);         
         
         postOffice.removeBinding("queue1", false);
         postOffice.removeBinding("queue2", false);
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }         
      }
   }
   
   private void routeWithFilter(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createNonClusteredPostOffice();
         
         SimpleFilter filter = new SimpleFilter(2);
         
         Condition condition1 = new SimpleCondition("topic1");
      
         MessagingQueue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, filter, false);
         queue1.activate();
         
         postOffice.addBinding(new Binding(condition1, queue1, false), false);
         
         MessagingQueue queue2 = new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue2.activate();
         
         postOffice.addBinding(new Binding(condition1, queue2, false), false);
         
         MessagingQueue queue3 = new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue3.activate();
         
         postOffice.addBinding(new Binding(condition1, queue3, false), false);
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.getLocalDistributor().add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.getLocalDistributor().add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.getLocalDistributor().add(receiver3);
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);         
         boolean routed = postOffice.route(ref1, condition1, null);      
         assertTrue(routed);
         Message msg2 = CoreMessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);         
         routed = postOffice.route(ref2, condition1, null);      
         assertTrue(routed);
         Message msg3 = CoreMessageFactory.createCoreMessage(3);      
         MessageReference ref3 = ms.reference(msg3);         
         routed = postOffice.route(ref3, condition1, null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);           
         assertEquals(1, msgs.size());                  
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver1.acknowledge(msgRec, null);
         msgs = queue1.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(3, msgs.size());
         Message msgRec1 = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec1);
         Message msgRec2 = (Message)msgs.get(1);
         assertTrue(msg2 == msgRec2);
         Message msgRec3 = (Message)msgs.get(2);
         assertTrue(msg3 == msgRec3);
          
         receiver2.acknowledge(msgRec1, null);
         receiver2.acknowledge(msgRec2, null);
         receiver2.acknowledge(msgRec3, null);
         msgs = queue2.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertEquals(3, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec1);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msg2 == msgRec2);
         msgRec3 = (Message)msgs.get(2);
         assertTrue(msg3 == msgRec3);
          
         receiver3.acknowledge(msgRec1, null);
         receiver3.acknowledge(msgRec2, null);
         receiver3.acknowledge(msgRec3, null);
         msgs = queue3.browse(null);
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());          
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }
        
      }
   }

   // Inner classes -------------------------------------------------

}

