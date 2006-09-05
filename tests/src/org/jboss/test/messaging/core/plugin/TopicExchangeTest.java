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
import org.jboss.messaging.core.plugin.exchange.TopicExchange;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.ExchangeTestBase;

/**
 * 
 * A TopicExchangeTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class TopicExchangeTest extends ExchangeTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicExchangeTest(String name)
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
      Exchange exchange = null;
      
      try
      {
      
         exchange = createExchange();
         
         Binding binding1 =
            exchange.bindQueue("sub1", "topic1", null, false, false, ms, pm, 1000, 20, 20);      
         Binding binding2 =
            exchange.bindQueue("sub2", "topic1", null, false, false, ms, pm, 1000, 20, 20);
         Binding binding3 =
            exchange.bindQueue("sub3", "topic1", null, false, false, ms, pm, 1000, 20, 20);      
         Binding binding4 =
            exchange.bindQueue("sub4", "topic2", null, false, true, ms, pm, 1000, 20, 20);
         Binding binding5 =
            exchange.bindQueue("sub5", "topic2", null, false, true, ms, pm, 1000, 20, 20);      
         Binding binding6 =
            exchange.bindQueue("sub6", "topic2", null, false, true, ms, pm, 1000, 20, 20);
         
         MessageQueue queue1 = binding1.getQueue();      
         MessageQueue queue2 = binding2.getQueue();
         MessageQueue queue3 = binding3.getQueue();      
         MessageQueue queue4 = binding4.getQueue();
         MessageQueue queue5 = binding5.getQueue();      
         MessageQueue queue6 = binding6.getQueue();
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue3.add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue4.add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue5.add(receiver5);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue6.add(receiver6);
         
         binding1.deactivate();
         binding2.deactivate();
         binding5.deactivate();
         binding6.deactivate();
         
         assertFalse(binding1.isActive());      
         assertFalse(binding2.isActive());
         assertFalse(binding5.isActive());
         assertFalse(binding6.isActive()); 
         assertTrue(binding3.isActive());
         assertTrue(binding4.isActive());      
         
         Message msg1 = MessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = exchange.route(ref1, "topic1", null);      
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
         msgs = queue3.browse();
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
                     
         Message msg2 = MessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = exchange.route(ref2, "topic2", null);      
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
         msgs = queue4.browse();
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
         if (exchange != null)
         {
            exchange.stop();
         }
      }
   
   }

   public final void testRouteNoBinding() throws Throwable
   {
      Exchange exchange = null;
      
      try
      {      
         exchange = createExchange();
         
         Binding binding1 =
            exchange.bindQueue("sub1", "topic1", null, false, false, ms, pm, 1000, 20, 20);      
   
         MessageQueue queue1 = binding1.getQueue();
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);
   
         assertTrue(binding1.isActive());
   
         Message msg1 = MessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = exchange.route(ref1, "this won't match anything", null);      
         
         //A topic exchange always returns true even if there is no binding
         assertTrue(routed);
               
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());             
         
      }
      finally
      {
         if (exchange != null)
         {
            exchange.stop();
         }
      }
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void route(boolean persistentMessage) throws Throwable
   {
      Exchange exchange = null;
      
      try
      {      
         exchange = createExchange();
      
         Binding binding1 =
            exchange.bindQueue("sub1", "topic1", null, false, false, ms, pm, 1000, 20, 20);      
         Binding binding2 =
            exchange.bindQueue("sub2", "topic1", null, false, false, ms, pm, 1000, 20, 20);
         Binding binding3 =
            exchange.bindQueue("sub3", "topic1", null, false, false, ms, pm, 1000, 20, 20);      
         Binding binding4 =
            exchange.bindQueue("sub4", "topic2", null, false, true, ms, pm, 1000, 20, 20);
         Binding binding5 =
            exchange.bindQueue("sub5", "topic2", null, false, true, ms, pm, 1000, 20, 20);      
         Binding binding6 =
            exchange.bindQueue("sub6", "topic2", null, false, true, ms, pm, 1000, 20, 20);
         
         MessageQueue queue1 = binding1.getQueue();      
         MessageQueue queue2 = binding2.getQueue();
         MessageQueue queue3 = binding3.getQueue();      
         MessageQueue queue4 = binding4.getQueue();
         MessageQueue queue5 = binding5.getQueue();      
         MessageQueue queue6 = binding6.getQueue();
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue3.add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue4.add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue5.add(receiver5);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue6.add(receiver6);
         
         assertTrue(binding1.isActive());      
         assertTrue(binding2.isActive());
         assertTrue(binding3.isActive());      
         assertTrue(binding4.isActive());
         assertTrue(binding5.isActive());      
         assertTrue(binding6.isActive());
         
         Message msg1 = MessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = exchange.route(ref1, "topic1", null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver1.acknowledge(msgRec, null);
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver2.acknowledge(msgRec, null);
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec);
         receiver3.acknowledge(msgRec, null);
         msgs = queue3.browse();
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
         
         
         Message msg2 = MessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = exchange.route(ref2, "topic2", null);      
         assertTrue(routed);
         
         msgs = receiver4.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver4.acknowledge(msgRec, null);
         msgs = queue4.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver5.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver5.acknowledge(msgRec, null);
         msgs = queue5.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = receiver6.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver6.acknowledge(msgRec, null);
         msgs = queue6.browse();
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
         
      }
      finally
      {
         if (exchange != null)
         {
            exchange.stop();
         }
      }
   }
   
   protected void routeTransactional(boolean persistentMessage) throws Throwable
   {
      Exchange exchange = null;
      
      try
      {      
         exchange = createExchange();
      
         Binding binding1 =
            exchange.bindQueue("sub1", "topic1", null, false, false, ms, pm, 1000, 20, 20);     
         Binding binding2 =
            exchange.bindQueue("sub2", "topic1", null, false, true, ms, pm, 1000, 20, 20);  
   
         MessageQueue queue1 = binding1.getQueue();
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);
         MessageQueue queue2 = binding2.getQueue();
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.add(receiver2);
   
         assertTrue(binding1.isActive());
         assertTrue(binding2.isActive());
   
         Message msg1 = MessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = MessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();
         
         boolean routed = exchange.route(ref1, "topic1", tx);            
         assertTrue(routed);
         routed = exchange.route(ref2, "topic1", tx);            
         assertTrue(routed);
               
         List msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
         
         msgs = queue2.browse();
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
         
         //Acknowledge non transactionally
         receiver1.acknowledge(msgRec1, null);
         receiver1.acknowledge(msgRec2, null);
         
         receiver2.acknowledge(msgRec1, null);
         receiver2.acknowledge(msgRec2, null);
   
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         receiver1.clear();
         
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         receiver2.clear();
              
         Message msg3 = MessageFactory.createCoreMessage(3, persistentMessage, null);      
         MessageReference ref3 = ms.reference(msg3);
         
         Message msg4 = MessageFactory.createCoreMessage(4, persistentMessage, null);      
         MessageReference ref4 = ms.reference(msg4);
         
         tx = tr.createTransaction();
         
         routed = exchange.route(ref3, "topic1", tx);            
         assertTrue(routed);
         routed = exchange.route(ref4, "topic1", tx);            
         assertTrue(routed);
               
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
         msgs = queue2.browse();
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
         
         
         Message msg5 = MessageFactory.createCoreMessage(5, persistentMessage, null);      
         MessageReference ref5 = ms.reference(msg5);
         
         Message msg6 = MessageFactory.createCoreMessage(6, persistentMessage, null);      
         MessageReference ref6 = ms.reference(msg6);
               
         routed = exchange.route(ref5, "topic1", null);            
         assertTrue(routed);
         routed = exchange.route(ref6, "topic1", null);            
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
         
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg5);
         assertTrue(msgRec2 == msg6);
         
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg5);
         assertTrue(msgRec2 == msg6);
         
         tx.commit();
         
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
               
         receiver1.clear();
         
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());
               
         receiver2.clear();
         
         Message msg7 = MessageFactory.createCoreMessage(7, persistentMessage, null);      
         MessageReference ref7 = ms.reference(msg7);
         
         Message msg8 = MessageFactory.createCoreMessage(8, persistentMessage, null);      
         MessageReference ref8 = ms.reference(msg8);
               
         routed = exchange.route(ref7, "topic1", null);            
         assertTrue(routed);
         routed = exchange.route(ref8, "topic1", null);            
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
         
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);
         
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);
         
         tx.rollback();
         
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);
         
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertEquals(2, msgs.size());
         msgRec1 = (Message)msgs.get(0);
         msgRec2 = (Message)msgs.get(1);
         assertTrue(msgRec1 == msg7);
         assertTrue(msgRec2 == msg8);        
      }
      finally
      {
         if (exchange != null)
         {
            exchange.stop();
         }
      }
   }
   
   protected Exchange createExchange() throws Exception
   {
      TopicExchange exchange = new TopicExchange(sc.getDataSource(), sc.getTransactionManager());      
      
      exchange.start();      
      
      ((TopicExchange)exchange).injectAttributes("Topic", "node1", ms, im, pool, tr);
      
      return exchange;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



