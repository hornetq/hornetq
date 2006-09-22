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
import java.util.List;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.NullMessagePullPolicy;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A DefaultRouterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultRouterTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected ServiceContainer sc;

   protected IdManager im;   
   
   protected PersistenceManager pm;
      
   protected MessageStore ms;
   
   protected TransactionRepository tr;
   
   protected QueuedExecutorPool pool;
   
   // Constructors --------------------------------------------------

   public DefaultRouterTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");
      
      sc.start();                
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      pool = new QueuedExecutorPool(10);
      
      im = new IdManager("CHANNEL_ID", 10, pm);
            
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
      tr.stop();
      ms.stop();
      
      super.tearDown();
   }
   
   public void testNotLocalPersistent() throws Throwable
   {
      notLocal(true);
   }
   
   public void testNotLocalNonPersistent() throws Throwable
   {
      notLocal(false);
   }
   
   public void testLocalPersistent() throws Throwable
   {
      local(true);
   }
   
   public void testLocalNonPersistent() throws Throwable
   {
      local(false);
   }
   
   protected void notLocal(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
      
      ClusteredPostOffice office6 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         office4 = createClusteredPostOffice("node4", "testgroup");
         
         office5 = createClusteredPostOffice("node5", "testgroup");
         
         office6 = createClusteredPostOffice("node6", "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office2, "node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding1 = office2.bindClusteredQueue("topic", queue1);
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office3, "node3", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding2 = office3.bindClusteredQueue("topic", queue2);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office4, "node4", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding3 = office4.bindClusteredQueue("topic", queue3);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office5, "node5", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding4 = office5.bindClusteredQueue("topic", queue4);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office6, "node6", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding5 = office6.bindClusteredQueue("topic", queue5);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
               
         List msgs = sendMessages(persistent, office1, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkContainsAndAcknowledge(msgs, receiver3, queue1);                           
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkContainsAndAcknowledge(msgs, receiver4, queue1);                                    
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkContainsAndAcknowledge(msgs, receiver5, queue1); 
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office1, 3, null);         
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
                     
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
         
         if (office6 != null)
         {
            office6.stop();
         }
      }
   }
   
   
   protected void local(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
      
      ClusteredPostOffice office6 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         office4 = createClusteredPostOffice("node4", "testgroup");
         
         office5 = createClusteredPostOffice("node5", "testgroup");
         
         office6 = createClusteredPostOffice("node6", "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office2, "node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding1 = office2.bindClusteredQueue("topic", queue1);
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office3, "node3", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding2 = office3.bindClusteredQueue("topic", queue2);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office4, "node4", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding3 = office4.bindClusteredQueue("topic", queue3);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office5, "node5", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding4 = office5.bindClusteredQueue("topic", queue4);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office6, "node6", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding5 = office6.bindClusteredQueue("topic", queue5);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
               
         List msgs = sendMessages(persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         
         msgs = sendMessages(persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages(persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
                     
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
         
         if (office6 != null)
         {
            office6.stop();
         }
      }
   }
   
   
   
   protected ClusteredPostOffice createClusteredPostOffice(String nodeId, String groupName) throws Exception
   {
      MessagePullPolicy redistPolicy = new NullMessagePullPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new DefaultRouterFactory();
      
      DefaultClusteredPostOffice postOffice = 
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 5000, 5000, redistPolicy, rf, 1);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------
   
   //TODO these methods are duplicated from DefaultClusteredPostOfficeTest - put in common super class or somewhere
   //else
   
   private List sendMessages(boolean persistent, PostOffice office, int num, Transaction tx) throws Exception
   {
      List list = new ArrayList();
      
      Message msg = CoreMessageFactory.createCoreMessage(1, persistent, null);      
      
      MessageReference ref = ms.reference(msg);         
      
      boolean routed = office.route(ref, "topic", null);         
      
      assertTrue(routed);
      
      list.add(msg);
      
      Thread.sleep(1000);
      
      return list;
   }
   
   
   private void checkContainsAndAcknowledge(Message msg, SimpleReceiver receiver, Queue queue) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      Message msgRec = (Message)msgs.get(0);
      assertEquals(msg.getMessageID(), msgRec.getMessageID());
      receiver.acknowledge(msgRec, null);
      msgs = queue.browse();
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty()); 
      receiver.clear();
   }
   
   private void checkContainsAndAcknowledge(List msgList, SimpleReceiver receiver, Queue queue) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertEquals(msgList.size(), msgs.size());
      
      for (int i = 0; i < msgList.size(); i++)
      {
         Message msgRec = (Message)msgs.get(i);
         Message msgCheck = (Message)msgList.get(i);
         assertEquals(msgCheck.getMessageID(), msgRec.getMessageID());
         receiver.acknowledge(msgRec, null);
      }
      
      msgs = queue.browse();
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty()); 
      receiver.clear();
   }
   
   private void checkEmpty(SimpleReceiver receiver) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty());
   }

   // Inner classes -------------------------------------------------
   

}



