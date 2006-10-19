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
package org.jboss.test.messaging.core.plugin.postoffice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleFilter;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;
import org.jboss.tm.TransactionManagerService;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A DefaultPostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultPostOfficeTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DefaultPostOfficeTest(String name)
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
   
   // Public --------------------------------------------------------
   
   
   public final void testBind() throws Throwable
   {
      PostOffice office1 = null;
      
      PostOffice office2 = null;
      
      PostOffice office3 = null;
      
      try
      {             
         office1 = createPostOffice();
         
         //Bind one durable
         
         Filter filter1 = new Selector("x = 'cheese'");
         Filter filter2 = new Selector("y = 'bread'");
         
         PagingFilteredQueue queue1 = new PagingFilteredQueue("durableQueue", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);
         
         
         Binding binding1 =
            office1.bindQueue("condition1", queue1);
         
         //Binding twice with the same name should fail      
         try
         {
            Binding bindFail = office1.bindQueue("condition1", queue1);
            fail();
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
               
         //Bind one non durable
         PagingFilteredQueue queue2 = new PagingFilteredQueue("nonDurableQueue", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding2 =
            office1.bindQueue("condition2", queue2);
         
         //Check they're there
         
         Binding binding3 = office1.getBindingForQueueName("durableQueue");
         assertNotNull(binding3);
         assertTrue(binding1 == binding3);
         assertEquivalent(binding1, binding3);
         assertTrue(binding3.getQueue().isActive());
         assertEquals(true, binding3.getQueue().isRecoverable());
         
         
         Binding binding4 = office1.getBindingForQueueName("nonDurableQueue");
         assertNotNull(binding4);
         assertTrue(binding2 == binding4);
         assertEquivalent(binding2, binding4);
         assertTrue(binding4.getQueue().isActive());
         assertEquals(false, binding4.getQueue().isRecoverable());
         
         office1.stop();
         
         //Throw away the office and create another
         office2 = createPostOffice();
         
         //Only one binding should be there
         Binding binding5 = office2.getBindingForQueueName("durableQueue");
         assertNotNull(binding5);
         assertEquivalent(binding1, binding5);
         //Should be inactive
         assertFalse(binding5.getQueue().isActive());
         
         Binding binding6 = office2.getBindingForQueueName("nonDurableQueue");
         assertNull(binding6);
         
         //Unbind the binding
         Binding binding7 = office2.unbindQueue("durableQueue");
         assertNotNull(binding7);
         assertEquivalent(binding1, binding7);
         
         //Make sure no longer there
         Binding binding8 = office2.getBindingForQueueName("durableQueue");
         assertNull(binding8);
         
         office2.stop();
         
         //Throw away office and start another
         office3 = createPostOffice();
         
         //Make sure not there
         Binding binding9 = office3.getBindingForQueueName("durableQueue");
         assertNull(binding9);
         
         
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
         
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      }
            
   }
   
   public final void testListBindings() throws Throwable
   {
      PostOffice office = null;
      
      try
      {      
         office = createPostOffice();
         
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 =
            office.bindQueue("condition1", queue1);
         
         PagingFilteredQueue queue2 = new PagingFilteredQueue("queue2", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding2 =
            office.bindQueue("condition1", queue2);
         
         PagingFilteredQueue queue3 = new PagingFilteredQueue("queue3", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding3 =
            office.bindQueue("condition1", queue3);
         
         PagingFilteredQueue queue4 = new PagingFilteredQueue("queue4", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding4 =
            office.bindQueue("condition1", queue4);
         
         PagingFilteredQueue queue5 = new PagingFilteredQueue("queue5", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding5 =
            office.bindQueue("condition2", queue5);
         
         PagingFilteredQueue queue6 = new PagingFilteredQueue("queue6", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding6 =
            office.bindQueue("condition2", queue6);
         
         PagingFilteredQueue queue7 = new PagingFilteredQueue("queue7", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding7 =
            office.bindQueue("condition2", queue7);
         
         PagingFilteredQueue queue8 = new PagingFilteredQueue("queue8", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding8 =
            office.bindQueue("condition2", queue8);
         
         
         Collection bindings = office.listBindingsForCondition("dummy");
         assertNotNull(bindings);
         assertTrue(bindings.isEmpty());
         
         //We don't match on substrings
         bindings = office.listBindingsForCondition("condition123");
         assertNotNull(bindings);
         assertTrue(bindings.isEmpty());
         
         //We don't currently support hierarchies
         bindings = office.listBindingsForCondition("condition1.subcondition");
         assertNotNull(bindings);
         assertTrue(bindings.isEmpty());
         
         //We currently just do an exact match
         bindings = office.listBindingsForCondition("condition1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         Iterator iter = bindings.iterator();
         assertEquivalent((Binding)iter.next(), binding1);
         assertEquivalent((Binding)iter.next(), binding2);
         assertEquivalent((Binding)iter.next(), binding3);
         assertEquivalent((Binding)iter.next(), binding4);
         
         bindings = office.listBindingsForCondition("condition2");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent((Binding)iter.next(), binding5);
         assertEquivalent((Binding)iter.next(), binding6);
         assertEquivalent((Binding)iter.next(), binding7);
         assertEquivalent((Binding)iter.next(), binding8);
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
      
         postOffice = createPostOffice();
         
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding1 =
            postOffice.bindQueue("topic1", queue1);
         
         PagingFilteredQueue queue2 = new PagingFilteredQueue("queue2", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding2 =
            postOffice.bindQueue("topic1", queue2);
         
         PagingFilteredQueue queue3 = new PagingFilteredQueue("queue3", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding3 =
            postOffice.bindQueue("topic1", queue3);
         
         PagingFilteredQueue queue4 = new PagingFilteredQueue("queue4", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding4 =
            postOffice.bindQueue("topic2", queue4);
         
         PagingFilteredQueue queue5 = new PagingFilteredQueue("queue5", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding5 =
            postOffice.bindQueue("topic2", queue5);
         
         PagingFilteredQueue queue6 = new PagingFilteredQueue("queue6", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding6 =
            postOffice.bindQueue("topic2", queue6);
      
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
         SimpleReceiver receiver6 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue6.add(receiver6);
         
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
         
         boolean routed = postOffice.route(ref1, "topic1", null);      
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
                     
         Message msg2 = CoreMessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = postOffice.route(ref2, "topic2", null);      
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
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
         if (checkNoMessageData())
         {
            fail("data still in database");
         }
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      }
   
   }

   public final void testRouteNoBinding() throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createPostOffice();
         
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding1 =
            postOffice.bindQueue("condition1", queue1);
              
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);
   
         assertTrue(queue1.isActive());
   
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = postOffice.route(ref1, "this won't match anything", null);      
         
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
         
         if (checkNoMessageData())
         {
            fail("data still in database");
         }
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      }
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void routeWithFilter(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createPostOffice();
         
         SimpleFilter filter = new SimpleFilter(2);
      
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), filter);         
         
         Binding binding1 =
            postOffice.bindQueue("topic1", queue1);
         
         PagingFilteredQueue queue2 = new PagingFilteredQueue("queue2", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding2 =
            postOffice.bindQueue("topic1", queue2);
         
         PagingFilteredQueue queue3 = new PagingFilteredQueue("queue3", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding3 =
            postOffice.bindQueue("topic1", queue3);   
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);         
         boolean routed = postOffice.route(ref1, "topic1", null);      
         assertTrue(routed);
         Message msg2 = CoreMessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);         
         routed = postOffice.route(ref2, "topic1", null);      
         assertTrue(routed);
         Message msg3 = CoreMessageFactory.createCoreMessage(3);      
         MessageReference ref3 = ms.reference(msg3);         
         routed = postOffice.route(ref3, "topic1", null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver1.acknowledge(msgRec, null);
         msgs = queue1.browse();
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
         msgs = queue2.browse();
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
         msgs = queue3.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
      }
      finally
      {
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
         if (checkNoMessageData())
         {
            fail("data still in database");
         }
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      }
   }
   
   protected void route(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createPostOffice();
      
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding1 =
            postOffice.bindQueue("topic1", queue1);
         
         PagingFilteredQueue queue2 = new PagingFilteredQueue("queue2", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding2 =
            postOffice.bindQueue("topic1", queue2);
         
         PagingFilteredQueue queue3 = new PagingFilteredQueue("queue3", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding3 =
            postOffice.bindQueue("topic1", queue3);
         
         PagingFilteredQueue queue4 = new PagingFilteredQueue("queue4", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         
         Binding binding4 =
            postOffice.bindQueue("topic2", queue4);
         
         PagingFilteredQueue queue5 = new PagingFilteredQueue("queue5", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         
         Binding binding5 =
            postOffice.bindQueue("topic2", queue5);
         
         PagingFilteredQueue queue6 = new PagingFilteredQueue("queue6", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         
         Binding binding6 =
            postOffice.bindQueue("topic2", queue6);
      
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
         
         assertTrue(queue1.isActive());      
         assertTrue(queue2.isActive());
         assertTrue(queue3.isActive());      
         assertTrue(queue4.isActive());
         assertTrue(queue5.isActive());      
         assertTrue(queue6.isActive());
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = postOffice.route(ref1, "topic1", null);      
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
         
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         routed = postOffice.route(ref2, "topic2", null);      
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
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
         if (checkNoMessageData())
         {
            fail("data still in database");
         }
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      
      }
   }
   
   protected void routeTransactional(boolean persistentMessage) throws Throwable
   {
      PostOffice postOffice = null;
      
      try
      {      
         postOffice = createPostOffice();
      
         PagingFilteredQueue queue1 = new PagingFilteredQueue("queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 =
            postOffice.bindQueue("topic1", queue1);
         
         PagingFilteredQueue queue2 = new PagingFilteredQueue("queue2", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);
         
         Binding binding2 =
            postOffice.bindQueue("topic1", queue2);
          
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue1.add(receiver1);

         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);;
         queue2.add(receiver2);
   
         assertTrue(queue1.isActive());
         assertTrue(queue2.isActive());
   
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();
         
         boolean routed = postOffice.route(ref1, "topic1", tx);            
         assertTrue(routed);
         routed = postOffice.route(ref2, "topic1", tx);            
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
              
         Message msg3 = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);      
         MessageReference ref3 = ms.reference(msg3);
         
         Message msg4 = CoreMessageFactory.createCoreMessage(4, persistentMessage, null);      
         MessageReference ref4 = ms.reference(msg4);
         
         tx = tr.createTransaction();
         
         routed = postOffice.route(ref3, "topic1", tx);            
         assertTrue(routed);
         routed = postOffice.route(ref4, "topic1", tx);            
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
         
         
         Message msg5 = CoreMessageFactory.createCoreMessage(5, persistentMessage, null);      
         MessageReference ref5 = ms.reference(msg5);
         
         Message msg6 = CoreMessageFactory.createCoreMessage(6, persistentMessage, null);      
         MessageReference ref6 = ms.reference(msg6);
               
         routed = postOffice.route(ref5, "topic1", null);            
         assertTrue(routed);
         routed = postOffice.route(ref6, "topic1", null);            
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
         
         Message msg7 = CoreMessageFactory.createCoreMessage(7, persistentMessage, null);      
         MessageReference ref7 = ms.reference(msg7);
         
         Message msg8 = CoreMessageFactory.createCoreMessage(8, persistentMessage, null);      
         MessageReference ref8 = ms.reference(msg8);
               
         routed = postOffice.route(ref7, "topic1", null);            
         assertTrue(routed);
         routed = postOffice.route(ref8, "topic1", null);            
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
         if (postOffice != null)
         {
            postOffice.stop();
         }
         
         if (checkNoMessageData())
         {
            fail("data still in database");
         };
         if (checkNoBindingData())
         {
            fail("Binding data still in database");
         }
      }
   }
   
   protected void assertEquivalent(Binding binding1, Binding binding2)
   {
      assertEquals(binding1.getNodeId(), binding2.getNodeId());
      assertEquals(binding1.getQueue().getName(), binding2.getQueue().getName()); 
      String selector1 = binding1.getQueue().getFilter() != null ? binding1.getQueue().getFilter().getFilterString() : null;
      String selector2 = binding2.getQueue().getFilter() != null ? binding2.getQueue().getFilter().getFilterString() : null;
      assertEquals(selector1, selector2);
      assertEquals(binding1.getQueue().getChannelID(), binding2.getQueue().getChannelID());
      assertEquals(binding1.getQueue().isRecoverable(), binding2.getQueue().isRecoverable());
   }
   
   
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

