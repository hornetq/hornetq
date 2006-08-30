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
import org.jboss.messaging.core.plugin.exchange.TopicExchange;
import org.jboss.messaging.core.tx.Transaction;
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
   
   public void testRoute() throws Throwable
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("sub1", "topic1", null, false, true, ms, pm, 1000, 20, 20);      
      Binding binding2 =
         exchange.bindQueue("sub2", "topic1", null, false, true, ms, pm, 1000, 20, 20);
      Binding binding3 =
         exchange.bindQueue("sub3", "topic1", null, false, true, ms, pm, 1000, 20, 20);      
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
      
      assertTrue(binding1.isActive());      
      assertTrue(binding2.isActive());
      assertTrue(binding3.isActive());      
      assertTrue(binding4.isActive());
      assertTrue(binding5.isActive());      
      assertTrue(binding6.isActive());
      
      Message msg1 = MessageFactory.createCoreMessage(1);      
      MessageReference ref1 = ms.reference(msg1);
      
      boolean routed = exchange.route(ref1, "topic1", null);      
      assertTrue(routed);
      
      List msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      Message msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg1.getMessageID());
      
      msgs = queue2.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg1.getMessageID());
      
      msgs = queue3.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg1.getMessageID());
      
      msgs = queue4.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue5.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue6.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      queue1.removeAllReferences();
      queue2.removeAllReferences();
      queue3.removeAllReferences();
      
      
      Message msg2 = MessageFactory.createCoreMessage(2);      
      MessageReference ref2 = ms.reference(msg2);
      
      routed = exchange.route(ref2, "topic2", null);      
      assertTrue(routed);
      
      msgs = queue4.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue5.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue6.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue2.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue3.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      
   }
   
   public void testRouteInactive() throws Throwable
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("sub1", "topic1", null, false, true, ms, pm, 1000, 20, 20);      
      Binding binding2 =
         exchange.bindQueue("sub2", "topic1", null, false, true, ms, pm, 1000, 20, 20);
      Binding binding3 =
         exchange.bindQueue("sub3", "topic1", null, false, true, ms, pm, 1000, 20, 20);      
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
      
      binding1.deactivate();
      binding2.deactivate();
      binding3.deactivate();
      
      assertFalse(binding1.isActive());      
      assertFalse(binding2.isActive());
      assertFalse(binding3.isActive());      
      assertTrue(binding4.isActive());
      assertTrue(binding5.isActive());      
      assertTrue(binding6.isActive());
      
      Message msg1 = MessageFactory.createCoreMessage(1);      
      MessageReference ref1 = ms.reference(msg1);
      
      boolean routed = exchange.route(ref1, "topic1", null);      
      assertTrue(routed);
      
      List msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue2.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue3.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());

      msgs = queue4.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue5.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue6.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      Message msg2 = MessageFactory.createCoreMessage(2);      
      MessageReference ref2 = ms.reference(msg2);
      
      routed = exchange.route(ref2, "topic2", null);      
      assertTrue(routed);
      
      msgs = queue4.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      Message msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue5.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue6.browse();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      msgRec = (Message)msgs.get(0);
      assertEquals(msgRec.getMessageID(), msg2.getMessageID());
      
      msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue2.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
      
      msgs = queue3.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
   
   }

   public void testRouteNoBinding() throws Throwable
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("sub1", "topic1", null, false, true, ms, pm, 1000, 20, 20);      

      MessageQueue queue1 = binding1.getQueue();

      assertTrue(binding1.isActive());

      Message msg1 = MessageFactory.createCoreMessage(1);      
      MessageReference ref1 = ms.reference(msg1);
      
      boolean routed = exchange.route(ref1, "this won't match anything", null);      
      
      //A topic exchange always returns true even if there is no binding
      assertTrue(routed);
            
      List msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());             
   }
   
   public void testRouteTransactional() throws Throwable
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("sub1", "topic1", null, false, true, ms, pm, 1000, 20, 20);      

      MessageQueue queue1 = binding1.getQueue();

      assertTrue(binding1.isActive());

      Message msg1 = MessageFactory.createCoreMessage(1);      
      MessageReference ref1 = ms.reference(msg1);
      
      Message msg2 = MessageFactory.createCoreMessage(2);      
      MessageReference ref2 = ms.reference(msg2);
      
      Transaction tx = tr.createTransaction();
      
      boolean routed = exchange.route(ref1, "topic1", tx);            
      assertTrue(routed);
      routed = exchange.route(ref2, "topic1", tx);            
      assertTrue(routed);
            
      List msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size()); 
      
      tx.commit();
      
      msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(2, msgs.size());
      Message msgRec1 = (Message)msgs.get(0);
      Message msgRec2 = (Message)msgs.get(0);
      assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
      assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
      
      queue1.removeAllReferences();
      
      Message msg3 = MessageFactory.createCoreMessage(3);      
      MessageReference ref3 = ms.reference(msg3);
      
      Message msg4 = MessageFactory.createCoreMessage(4);      
      MessageReference ref4 = ms.reference(msg4);
      
      Transaction tx2 = tr.createTransaction();
      
      routed = exchange.route(ref3, "topic1", tx2);            
      assertTrue(routed);
      routed = exchange.route(ref4, "topic1", tx2);            
      assertTrue(routed);
            
      msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size()); 
      
      tx.rollback();
      
      msgs = queue1.browse();
      assertNotNull(msgs);
      assertEquals(0, msgs.size());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected Exchange createExchange() throws Exception
   {
      TopicExchange exchange = new TopicExchange(sc.getDataSource(), sc.getTransactionManager());      
      
      ((TopicExchange)exchange).injectAttributes("Topic", "node1", ms, im, pool, tr);
      
      exchange.start();
      
      return exchange;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



