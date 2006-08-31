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
package org.jboss.test.messaging.core.plugin.base;

import java.util.List;

import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.Exchange;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * 
 * A ExchangeTestBase

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class ExchangeTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   protected PersistenceManager pm;
      
   protected MessageStore ms;
   
   protected IdManager im;
   
   protected TransactionRepository tr;
   
   protected QueuedExecutorPool pool;
   
   // Constructors --------------------------------------------------

   public ExchangeTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");
      
      sc.start();                
      
      pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
      
      pm.start();
      
      ms = new SimpleMessageStore();
      
      im = new IdManager("CHANNEL_ID", 10, pm);
      
      tr = new TransactionRepository();
      
      tr.injectAttributes(pm, new IdManager("TRANSACTION_ID", 10, pm));
      
      pool = new QueuedExecutorPool(10);
            
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }
      pm.stop();
      
      super.tearDown();
   }
   
   // Public --------------------------------------------------------
   
   public final void testBind() throws Throwable
   {
      Exchange exchange1 = createExchange();
      
      //Bind one durable
      
      Filter filter1 = new Selector("x = 'cheese'");
      Filter filter2 = new Selector("y = 'bread'");
      
      Binding binding1 =
         exchange1.bindQueue("durableQueue", "condition1", filter1, true, true, ms, pm, 2000, 100, 100);
      
      //Binding twice with the same name should fail      
      try
      {
         Binding bindFail = exchange1.bindQueue("durableQueue", "condition1", filter1, true, true, ms, pm, 2000, 100, 100);
         fail();
      }
      catch (Exception e)
      {
         //Ok
      }
            
      //Bind one non durable
      Binding binding2 =
         exchange1.bindQueue("nonDurableQueue", "condition2", filter2, true, false, ms, pm, 2000, 100, 100);
      
      //Check they're there
      
      Binding binding3 = exchange1.getBindingForName("durableQueue");
      assertNotNull(binding3);
      assertTrue(binding1 == binding3);
      assertEquivalent(binding1, binding3);
      assertNotNull(binding3.getQueue());
      assertEquals(true, binding3.getQueue().isRecoverable());
      
      
      Binding binding4 = exchange1.getBindingForName("nonDurableQueue");
      assertNotNull(binding4);
      assertTrue(binding2 == binding4);
      assertEquivalent(binding2, binding4);
      assertNotNull(binding4.getQueue());
      assertEquals(false, binding4.getQueue().isRecoverable());
      
      //Throw away the exchange and create another
      Exchange exchange2 = createExchange();
      
      //Only one binding should be there
      Binding binding5 = exchange2.getBindingForName("durableQueue");
      assertNotNull(binding5);
      assertEquivalent(binding1, binding5);
      //Should be unloaded
      assertNull(binding5.getQueue());
      
      Binding binding6 = exchange2.getBindingForName("nonDurableQueue");
      assertNull(binding6);
      
      //ReLoad the durable queue - or we won't be able to unbind it
      exchange2.reloadQueues("condition1", ms, pm, 2000, 100, 100);
      
      //Unbind the binding
      Binding binding7 = exchange2.unbindQueue("durableQueue");
      assertNotNull(binding7);
      assertEquivalent(binding1, binding7);
      
      //Make sure no longer there
      Binding binding8 = exchange2.getBindingForName("durableQueue");
      assertNull(binding8);
      
      //Throw away exchange and start another
      Exchange exchange3 = createExchange();
      
      //Make sure not there
      Binding binding9 = exchange3.getBindingForName("durableQueue");
      assertNull(binding9);
            
   }
   
   public final void testListBindings() throws Throwable
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("queue1", "condition1", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding2 =
         exchange.bindQueue("queue2", "condition1", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding3 =
         exchange.bindQueue("queue3", "condition1", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding4 =
         exchange.bindQueue("queue4", "condition1", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding5 =
         exchange.bindQueue("queue5", "condition2", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding6 =
         exchange.bindQueue("queue6", "condition2", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding7 =
         exchange.bindQueue("queue7", "condition2", null, true, false, ms, pm, 2000, 100, 100);
      Binding binding8 =
         exchange.bindQueue("queue8", "condition2", null, true, false, ms, pm, 2000, 100, 100);
      
      List bindings = exchange.listBindingsForWildcard("dummy");
      assertNotNull(bindings);
      assertTrue(bindings.isEmpty());
      
      //We don't match on substrings
      bindings = exchange.listBindingsForWildcard("condition123");
      assertNotNull(bindings);
      assertTrue(bindings.isEmpty());
      
      //We don't currently support hierarchies
      bindings = exchange.listBindingsForWildcard("condition1.subcondition");
      assertNotNull(bindings);
      assertTrue(bindings.isEmpty());
      
      //We currently just do an exact match
      bindings = exchange.listBindingsForWildcard("condition1");
      assertNotNull(bindings);
      assertEquals(4, bindings.size());
      
      assertEquivalent((Binding)bindings.get(0), binding1);
      assertEquivalent((Binding)bindings.get(1), binding2);
      assertEquivalent((Binding)bindings.get(2), binding3);
      assertEquivalent((Binding)bindings.get(3), binding4);
      
      bindings = exchange.listBindingsForWildcard("condition2");
      assertNotNull(bindings);
      assertEquals(4, bindings.size());
      
      assertEquivalent((Binding)bindings.get(0), binding5);
      assertEquivalent((Binding)bindings.get(1), binding6);
      assertEquivalent((Binding)bindings.get(2), binding7);
      assertEquivalent((Binding)bindings.get(3), binding8);
         
   }
   
   public final void testUnloadReload() throws Exception
   {
      Exchange exchange = createExchange();
      
      Binding binding1 =
         exchange.bindQueue("reloadqueue1", "condition3", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding2 =
         exchange.bindQueue("reloadqueue2", "condition3", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding3 =
         exchange.bindQueue("reloadqueue3", "condition3", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding4 =
         exchange.bindQueue("reloadqueue4", "condition3", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding5 =
         exchange.bindQueue("reloadqueue5", "condition4", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding6 =
         exchange.bindQueue("reloadqueue6", "condition4", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding7 =
         exchange.bindQueue("reloadqueue7", "condition4", null, true, true, ms, pm, 2000, 100, 100);
      Binding binding8 =
         exchange.bindQueue("reloadqueue8", "condition4", null, true, true, ms, pm, 2000, 100, 100);
      
      assertNotNull(binding1.getQueue());
      assertTrue(binding1.isActive());      
      assertNotNull(binding2.getQueue());
      assertTrue(binding2.isActive());
      assertNotNull(binding3.getQueue());
      assertTrue(binding3.isActive());
      assertNotNull(binding4.getQueue());
      assertTrue(binding4.isActive());
      
      assertNotNull(binding5.getQueue());
      assertTrue(binding5.isActive());
      assertNotNull(binding6.getQueue());
      assertTrue(binding6.isActive());
      assertNotNull(binding7.getQueue());
      assertTrue(binding7.isActive());
      assertNotNull(binding8.getQueue());
      assertTrue(binding8.isActive());
      
      exchange.unloadQueues("condition3");
      assertNull(binding1.getQueue());
      assertFalse(binding1.isActive());      
      assertNull(binding2.getQueue());
      assertFalse(binding2.isActive());
      assertNull(binding3.getQueue());
      assertFalse(binding3.isActive());
      assertNull(binding4.getQueue());
      assertFalse(binding4.isActive());
      
      assertNotNull(binding5.getQueue());
      assertTrue(binding5.isActive());
      assertNotNull(binding6.getQueue());
      assertTrue(binding6.isActive());
      assertNotNull(binding7.getQueue());
      assertTrue(binding7.isActive());
      assertNotNull(binding8.getQueue());
      assertTrue(binding8.isActive());
      
      exchange.unloadQueues("condition4");
      assertNull(binding1.getQueue());
      assertFalse(binding1.isActive());      
      assertNull(binding2.getQueue());
      assertFalse(binding2.isActive());
      assertNull(binding3.getQueue());
      assertFalse(binding3.isActive());
      assertNull(binding4.getQueue());
      assertFalse(binding4.isActive());
      
      assertNull(binding5.getQueue());
      assertFalse(binding5.isActive());
      assertNull(binding6.getQueue());
      assertFalse(binding6.isActive());
      assertNull(binding7.getQueue());
      assertFalse(binding7.isActive());
      assertNull(binding8.getQueue());
      assertFalse(binding8.isActive());
      
      exchange.reloadQueues("condition3", ms, pm, 2000, 100, 100);
      assertNotNull(binding1.getQueue());
      assertTrue(binding1.isActive());      
      assertNotNull(binding2.getQueue());
      assertTrue(binding2.isActive());
      assertNotNull(binding3.getQueue());
      assertTrue(binding3.isActive());
      assertNotNull(binding4.getQueue());
      assertTrue(binding4.isActive());
      
      assertNull(binding5.getQueue());
      assertFalse(binding5.isActive());
      assertNull(binding6.getQueue());
      assertFalse(binding6.isActive());
      assertNull(binding7.getQueue());
      assertFalse(binding7.isActive());
      assertNull(binding8.getQueue());
      assertFalse(binding8.isActive());
      
      exchange.reloadQueues("condition4", ms, pm, 2000, 100, 100);
      
      assertNotNull(binding1.getQueue());
      assertTrue(binding1.isActive());      
      assertNotNull(binding2.getQueue());
      assertTrue(binding2.isActive());
      assertNotNull(binding3.getQueue());
      assertTrue(binding3.isActive());
      assertNotNull(binding4.getQueue());
      assertTrue(binding4.isActive());
      
      assertNotNull(binding5.getQueue());
      assertTrue(binding5.isActive());
      assertNotNull(binding6.getQueue());
      assertTrue(binding6.isActive());
      assertNotNull(binding7.getQueue());
      assertTrue(binding7.isActive());
      assertNotNull(binding8.getQueue());
      assertTrue(binding8.isActive());
   }
   
   


   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected abstract Exchange createExchange() throws Exception;
   
   protected void assertEquivalent(Binding binding1, Binding binding2)
   {
      assertEquals(binding1.getNodeId(), binding2.getNodeId());
      assertEquals(binding1.getQueueName(), binding2.getQueueName());
      assertEquals(binding1.getSelector(), binding2.getSelector());
      assertEquals(binding1.getChannelId(), binding2.getChannelId());
      assertEquals(binding1.isDurable(), binding2.isDurable());
      assertEquals(binding1.isNoLocal(), binding2.isNoLocal());
   }
   
//   protected void checkHasMessages(Message[] msgArray, MessageQueue queue, SimpleReceiver receiver)
//   {
//      List msgs = receiver.getMessages();
//      
//      assertNotNull(msgs);
//      assertEquals(msgArray.length, msgs.size());
//      for (int i = 0; i < msgArray.length; i++)
//      {
//         assertEquals(msgArray[i].getMessageID(), ((Message)msgs.get(i)).getMessageID());
//      }
//      
//      msgs = queue.browse();
//      assertNotNull(msgs);
//      assertEquals(msgArray.length, msgs.size());
//      for (int i = 0; i < msgArray.length; i++)
//      {
//         assertEquals(msgArray[i].getMessageID(), ((Message)msgs.get(i)).getMessageID());
//      }
//   }
//   
//   protected void ackMessages(Message[] msgArray, SimpleReceiver receiver, Transaction tx) throws Throwable
//   {
//      for (int i = 0; i < msgArray.length; i++)
//      {
//         receiver.acknowledge(msgArray[i], tx);
//      }
//   }
//   
//   protected void checkHasntMessages(MessageQueue queue, SimpleReceiver receiver)
//   {
//      List msgs = receiver.getMessages();      
//      assertNotNull(msgs);
//      assertTrue(msgs.isEmpty());
//      
//      msgs = queue.browse();
//      assertNotNull(msgs);
//      assertTrue(msgs.isEmpty());
//   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}

