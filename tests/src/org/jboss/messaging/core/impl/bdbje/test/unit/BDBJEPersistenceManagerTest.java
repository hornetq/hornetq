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
package org.jboss.messaging.core.impl.bdbje.test.unit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.impl.BindingImpl;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.impl.QueueFactoryImpl;
import org.jboss.messaging.core.impl.QueueImpl;
import org.jboss.messaging.core.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.core.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.core.impl.bdbje.BDBJEPersistenceManager;
import org.jboss.messaging.core.impl.bdbje.test.unit.fakes.FakeBDBJEEnvironment;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A BDBJEPersistenceManagerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BDBJEPersistenceManagerTest extends UnitTestCase
{
   protected static final String ENV_DIR = "test-env";
   
   protected BDBJEPersistenceManager pm;
      
   protected BDBJEEnvironment bdb;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      bdb = new FakeBDBJEEnvironment();
      
      bdb.setEnvironmentPath(ENV_DIR);
      
      bdb.start();
      
      pm = new BDBJEPersistenceManager();
      
      pm.setEnvironment(bdb);
      
      pm.start();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      pm.stop();
   }
   
   private Queue createQueue(int i)
   {
      return new QueueImpl(i, "blah" + i, null, false, true, false, -1);
   }
   
   // The tests ----------------------------------------------------------------
              
   public void testAddMessage() throws Exception
   {      
      Queue queue = createQueue(67);
            
      Message m = createMessageWithRefs(1, queue);
      
      pm.addMessage(m);
      
      assertMessageInStore(m, queue); 
      
      try
      {
         pm.addMessage(null);
         
         fail("Should throw exception");
      }
      catch (NullPointerException ok)
      {
         //ok
      }
   }
   
   public void testAddNonReliableMessage() throws Exception
   {      
      Queue queue = createQueue(67);
            
      Message m = createMessageWithRefs(1, queue);
      
      m.setDurable(false);
      
      pm.addMessage(m);
      
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      BDBJEDatabase refDB = bdb.getDatabase(BDBJEPersistenceManager.REFERENCE_DB_NAME);
      
      byte[] msgBytes = msgDB.get(m.getMessageID());
      
      assertNull(msgBytes);
      
      byte[] refBytes = refDB.get(m.getMessageID());
      
      assertNull(refBytes);      
   }
         
   public void testDeleteReference() throws Exception
   {            
      Queue queue = createQueue(67);
      
      Message m = createMessageWithRefs(1, queue);
      
      List<MessageReference> refs = new ArrayList<MessageReference>(m.getReferences());
      
      
      pm.addMessage(m);
      
      assertMessageInStore(m, queue);
      
      
      pm.deleteReference(refs.get(2));
      
      assertMessageInStore(m, queue);
                      
      assertEquals(3, m.getReferences().size());
      
      assertTrue(m.getReferences().contains(refs.get(0)));
      assertTrue(m.getReferences().contains(refs.get(1)));
      assertTrue(m.getReferences().contains(refs.get(3)));
      
      
      pm.deleteReference(refs.get(1));
      
      assertMessageInStore(m, queue);
      
      assertEquals(2, m.getReferences().size());
      
      assertTrue(m.getReferences().contains(refs.get(0)));
      assertTrue(m.getReferences().contains(refs.get(3)));
      
      
                
      pm.deleteReference(refs.get(3));
      
      assertMessageInStore(m, queue);
      
      assertEquals(1, m.getReferences().size());
      
      assertTrue(m.getReferences().contains(refs.get(0)));
            
      
      pm.deleteReference(refs.get(0));
            
      assertMessageNotInStore(m);         
      
      assertStoreEmpty();
      
      try
      {
         pm.deleteReference(null);
         
         fail("Should throw exception");
      }
      catch (NullPointerException ok)
      {
         //ok
      }
   }
   
   public void testCommitTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = createQueue(67);
            
      Message m1 = createMessageWithRefs(1, queue);
      List<MessageReference> m1Refs = new ArrayList<MessageReference>(m1.getReferences());
    
      msgs.add(m1);
      
      Message m2 = createMessageWithRefs(2, queue);
      
      msgs.add(m2);
      
      Message m3 = createMessageWithRefs(3, queue);
      List<MessageReference> m3Refs = new ArrayList<MessageReference>(m3.getReferences());
      
      msgs.add(m3);
      
      pm.commitTransaction(msgs, null);
       
      assertMessageInStore(m1, queue);
      
      assertMessageInStore(m2, queue);
      
      assertMessageInStore(m3, queue);
      
      
      //Add a couple more
      
      List<Message> msgsMore = new ArrayList<Message>();
            
      Message m4 = createMessageWithRefs(4, queue);      
      msgsMore.add(m4);
      
      Message m5 = createMessageWithRefs(5, queue);      
      msgsMore.add(m5);
      
      //Delete some refs
      
      List<MessageReference> refsToRemove = new ArrayList<MessageReference>();
      
      refsToRemove.add(m1.getReferences().get(0));
      refsToRemove.add(m1.getReferences().get(3));
      
      refsToRemove.add(m2.getReferences().get(0));
      refsToRemove.add(m2.getReferences().get(1));
      refsToRemove.add(m2.getReferences().get(2));
      refsToRemove.add(m2.getReferences().get(3));
      
      refsToRemove.add(m3.getReferences().get(2));
      
      pm.commitTransaction(msgsMore, refsToRemove);
      
      assertMessageInStore(m1, queue);
      assertEquals(2, m1.getReferences().size());
      assertTrue(m1.getReferences().contains(m1Refs.get(1)));
      assertTrue(m1.getReferences().contains(m1Refs.get(2)));
      
      assertMessageNotInStore(m2);
      
      assertMessageInStore(m3, queue);
      assertEquals(3, m3.getReferences().size());
      assertTrue(m3.getReferences().contains(m3Refs.get(0)));
      assertTrue(m3.getReferences().contains(m3Refs.get(1)));
      assertTrue(m3.getReferences().contains(m3Refs.get(3)));
      
      assertMessageInStore(m4, queue);
      assertEquals(4, m4.getReferences().size());
      
      assertMessageInStore(m5, queue);
      assertEquals(4, m5.getReferences().size());
      
      //Delete the rest
      refsToRemove.clear();
      refsToRemove.addAll(m1.getReferences());
      refsToRemove.addAll(m3.getReferences());
      refsToRemove.addAll(m4.getReferences());
      refsToRemove.addAll(m5.getReferences());
      
      pm.commitTransaction(null, refsToRemove);
      
      assertMessageNotInStore(m1);
      assertMessageNotInStore(m2);
      assertMessageNotInStore(m4);
      assertMessageNotInStore(m5);
      assertMessageNotInStore(m5);
      
      //try with nulls
      try
      {
         pm.commitTransaction(null, null);
         
         fail("Should throw exception");
      }      
      catch (IllegalArgumentException e)
      {
         //OK
      }
      
   }     
   
   public void testCommitTransactionMixtureOfPersistentAndNonPersistent() throws Exception
   {      
      Message msg = generateMessage(1);
      
      Queue queue1 = new QueueImpl(1, "queue1", null, false, true, false, -1);
      assertTrue(queue1.isDurable());
      
      Queue queue2 = new QueueImpl(1, "queue1", null, false, false, false, -1);
      assertFalse(queue2.isDurable());
      
      Queue queue3 = new QueueImpl(1, "queue1", null, false, true, false, -1);
      assertTrue(queue3.isDurable());
      
      Queue queue4 = new QueueImpl(1, "queue1", null, false, false, false, -1);
      assertFalse(queue4.isDurable());
     
      MessageReference ref1 = msg.createReference(queue1);
      
      MessageReference ref2 = msg.createReference(queue2);
      
      MessageReference ref3 = msg.createReference(queue3);
      
      MessageReference ref4 = msg.createReference(queue4);
      
      List<Message> msgs = new ArrayList<Message>();
      
      msgs.add(msg);
            
      pm.commitTransaction(msgs, null);
      
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      BDBJEDatabase refDB = bdb.getDatabase(BDBJEPersistenceManager.REFERENCE_DB_NAME);
      
      byte[] msgBytes = msgDB.get(msg.getMessageID());
      
      assertNotNull(msgBytes);
      
      byte[] refBytes = refDB.get(msg.getMessageID());
      
      assertNotNull(refBytes);
      
      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      
      queues.put(queue1.getPersistenceID(), queue1);
      
      queues.put(queue2.getPersistenceID(), queue2);
      
      queues.put(queue3.getPersistenceID(), queue3);
      
      queues.put(queue4.getPersistenceID(), queue4);
      
      Message msg2 = extractMessage(queues, msg.getMessageID(), msgBytes, refBytes);
      
      //Should only have two refs since only two of the queues were durable
      
      List<MessageReference> refs = msg2.getReferences();
      
      assertEquals(2, refs.size());
      
      List<MessageReference> refsToRemove = new ArrayList<MessageReference>();
      
      refsToRemove.add(ref1);
      
      refsToRemove.add(ref2);
      
      refsToRemove.add(ref3);
      
      refsToRemove.add(ref4);
      
      pm.commitTransaction(null, refsToRemove);
      
      msgBytes = msgDB.get(msg.getMessageID());
      
      assertNull(msgBytes);
      
      refBytes = refDB.get(msg.getMessageID());
      
      assertNull(refBytes);
   }     
   
   
   
   public void testPrepareAndCommitTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = createQueue(67);
           
      Message m1 = createMessageWithRefs(1, queue);
      List<MessageReference> m1Refs = new ArrayList<MessageReference>(m1.getReferences());
    
      msgs.add(m1);
      
      Message m2 = createMessageWithRefs(2, queue);

      msgs.add(m2);
      
      Message m3 = createMessageWithRefs(3, queue);
      List<MessageReference> m3Refs = new ArrayList<MessageReference>(m3.getReferences());
      
      msgs.add(m3);
      
      pm.commitTransaction(msgs, null);
       
      assertMessageInStore(m1, queue);
      
      assertMessageInStore(m2, queue);
      
      assertMessageInStore(m3, queue);
      
      
      //Add a couple more
      
      List<Message> msgsMore = new ArrayList<Message>();
            
      Message m4 = createMessageWithRefs(4, queue);
      msgsMore.add(m4);
      
      Message m5 = createMessageWithRefs(5, queue);

      msgsMore.add(m5);
      
      //Delete some refs
      
      List<MessageReference> refsToRemove = new ArrayList<MessageReference>();
      
      refsToRemove.add(m1.getReferences().get(0));
      refsToRemove.add(m1.getReferences().get(3));
      
      refsToRemove.add(m2.getReferences().get(0));
      refsToRemove.add(m2.getReferences().get(1));
      refsToRemove.add(m2.getReferences().get(2));
      refsToRemove.add(m2.getReferences().get(3));
      
      refsToRemove.add(m3.getReferences().get(2));
      
      Xid xid = generateXid();
      
      pm.prepareTransaction(xid, msgsMore, refsToRemove);
      
      pm.commitPreparedTransaction(xid);
      
      assertMessageInStore(m1, queue);
      assertEquals(2, m1.getReferences().size());
      assertTrue(m1.getReferences().contains(m1Refs.get(1)));
      assertTrue(m1.getReferences().contains(m1Refs.get(2)));
      
      assertMessageNotInStore(m2);
      
      assertMessageInStore(m3, queue);
      assertEquals(3, m3.getReferences().size());
      assertTrue(m3.getReferences().contains(m3Refs.get(0)));
      assertTrue(m3.getReferences().contains(m3Refs.get(1)));
      assertTrue(m3.getReferences().contains(m3Refs.get(3)));
      
      assertMessageInStore(m4, queue);
      assertEquals(4, m4.getReferences().size());
      
      assertMessageInStore(m5, queue);
      assertEquals(4, m5.getReferences().size());
      
      //Delete the rest
      refsToRemove.clear();
      refsToRemove.addAll(m1.getReferences());
      refsToRemove.addAll(m3.getReferences());
      refsToRemove.addAll(m4.getReferences());
      refsToRemove.addAll(m5.getReferences());
      
      xid = generateXid();
      
      pm.prepareTransaction(xid, null, refsToRemove);
      
      pm.commitPreparedTransaction(xid);
      
      assertMessageNotInStore(m1);
      assertMessageNotInStore(m2);
      assertMessageNotInStore(m4);
      assertMessageNotInStore(m5);
      assertMessageNotInStore(m5);
      
      //try with nulls
      xid = generateXid();
      try
      {
         pm.prepareTransaction(xid, null, null);
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
   }     
      
   public void testPrepareAndCommitTransactionWithNoPersistentMessages() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = createQueue(67);
           
      Message m1 = createMessageWithRefs(1, queue);
      m1.setDurable(false);
      List<MessageReference> m1Refs = new ArrayList<MessageReference>(m1.getReferences());
    
      msgs.add(m1);
      
      Message m2 = createMessageWithRefs(2, queue);
      m2.setDurable(false);

      msgs.add(m2);
      
      Message m3 = createMessageWithRefs(3, queue);
      m3.setDurable(false);
      List<MessageReference> m3Refs = new ArrayList<MessageReference>(m3.getReferences());
      
      msgs.add(m3);
      
      //Delete some refs
      
      List<MessageReference> refsToRemove = new ArrayList<MessageReference>();
      
      refsToRemove.add(m1.getReferences().get(0));
      refsToRemove.add(m1.getReferences().get(3));
      
      refsToRemove.add(m2.getReferences().get(0));
      refsToRemove.add(m2.getReferences().get(1));
      refsToRemove.add(m2.getReferences().get(2));
      refsToRemove.add(m2.getReferences().get(3));
      
      refsToRemove.add(m3.getReferences().get(2));
      
      Xid xid = generateXid();
      
      try
      {
         //Nothing to be persisted in the tx - which is illegal
         
         pm.prepareTransaction(xid, msgs, refsToRemove);
         
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
            
      
   }     
   
   public void testPrepareAndUnprepareTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = createQueue(67);
        
      Message m1 = createMessageWithRefs(1, queue);

      msgs.add(m1);
      
      Message m2 = createMessageWithRefs(2, queue);

      msgs.add(m2);
      
      Message m3 = createMessageWithRefs(3, queue);

      msgs.add(m3);
      
      pm.commitTransaction(msgs, null);
       
      assertMessageInStore(m1, queue);
      
      assertMessageInStore(m2, queue);
      
      assertMessageInStore(m3, queue);
      
      
      //Add a couple more
      
      List<Message> msgsMore = new ArrayList<Message>();
            
      Message m4 = createMessageWithRefs(4, queue);
      msgsMore.add(m4);
      
      Message m5 = createMessageWithRefs(5, queue);
      msgsMore.add(m5);
      
      //Delete some refs
      
      List<MessageReference> refsToRemove = new ArrayList<MessageReference>();
      
      refsToRemove.add(m1.getReferences().get(0));
      refsToRemove.add(m1.getReferences().get(3));
      
      refsToRemove.add(m2.getReferences().get(0));
      refsToRemove.add(m2.getReferences().get(1));
      refsToRemove.add(m2.getReferences().get(2));
      refsToRemove.add(m2.getReferences().get(3));
      
      refsToRemove.add(m3.getReferences().get(2));
      
      Xid xid = generateXid();
      
      pm.prepareTransaction(xid, msgsMore, refsToRemove);
      
      pm.unprepareTransaction(xid, msgsMore, refsToRemove);
                  
      assertNumMessagesInStore(3);      
   }     
   
   public void testUpdateDeliveryCount() throws Exception
   {
      Queue queue = createQueue(67);
      
      Message m1 = createMessageWithRefs(1, queue);
      
      assertEquals(0, m1.getReferences().get(0).getDeliveryCount());
      assertEquals(0, m1.getReferences().get(1).getDeliveryCount());
      assertEquals(0, m1.getReferences().get(2).getDeliveryCount());
      assertEquals(0, m1.getReferences().get(3).getDeliveryCount());
      
      pm.addMessage(m1);
      
      final int delCount = 77;
      m1.getReferences().get(1).setDeliveryCount(delCount);
      pm.updateDeliveryCount(queue, m1.getReferences().get(1));
      
      final int delCount2 = 423;
      
      m1.getReferences().get(3).setDeliveryCount(delCount2);
      pm.updateDeliveryCount(queue, m1.getReferences().get(3));
      
      assertMessageInStore(m1, queue);
   }
   
   public void testRefsWithDifferentQueues() throws Exception
   {
      final int numQueues = 10;
      
      List<Message> msgs = new ArrayList<Message>();
                  
      for (int i = 0; i < numQueues; i++)
      {
         Queue queue = createQueue(i);
         
         MessageReference ref = generateReference(queue, i);
         
         msgs.add(ref.getMessage());
         
         pm.addMessage(ref.getMessage());   
         
         assertEquals(queue, ref.getQueue());
      }
      
      for (Message msg: msgs)
      {
         assertMessageInStore(msg, msg.getReferences().get(0).getQueue());
      }            
   }
       
   public void testAddRemoveBindings() throws Exception
   {
      Queue queue1 = new QueueImpl(1, "queue1", new FilterImpl("a=1"), false, true, false, -1);
      
      Queue queue2 = new QueueImpl(2, "queue2", new FilterImpl("a=1"), false, true, false, -1);
            
      Queue queue3 = new QueueImpl(3, "queue3", new FilterImpl("a=1"), false, true, false, -1);
      
      Queue queue4 = new QueueImpl(4, "queue4", new FilterImpl("a=1"), false, true, false, -1);
      
      String condition1 = "queue.condition1";
      
      Binding binding1 = new BindingImpl(1, condition1, queue1);
      
      String condition2 = "queue.condition2";
      
      Binding binding2 = new BindingImpl(1, condition2, queue2);
      
      String condition3 = "queue.condition3";
      
      Binding binding3 = new BindingImpl(3, condition3, queue3);
      
      //same condition
      String condition4 = "queue.condition3";
      
      Binding binding4 = new BindingImpl(3, condition4, queue4);
      
      
      pm.addBinding(binding1);
      
      pm.addBinding(binding2);
      
      pm.addBinding(binding3);
      
      pm.addBinding(binding4);
      
      List<Binding> bindings = pm.loadBindings(new QueueFactoryImpl());
      
      assertEquals(4, bindings.size());
      
      assertEquivalent(bindings.get(0), binding1);
      
      assertEquivalent(bindings.get(1), binding2);
      
      assertEquivalent(bindings.get(2), binding3);
      
      assertEquivalent(bindings.get(3), binding4);
      
      assertEquals(0, bindings.get(0).getQueue().getMessageCount());
      
      assertEquals(0, bindings.get(0).getQueue().getPersistenceID());
      
      assertEquals(0, bindings.get(1).getQueue().getMessageCount());
      
      assertEquals(1, bindings.get(1).getQueue().getPersistenceID());
      
      assertEquals(0, bindings.get(2).getQueue().getMessageCount());
      
      assertEquals(2, bindings.get(2).getQueue().getPersistenceID());
      
      assertEquals(0, bindings.get(3).getQueue().getMessageCount());
      
      assertEquals(3, bindings.get(3).getQueue().getPersistenceID());
      
      
      pm.deleteBinding(binding2);
      
      bindings = pm.loadBindings(new QueueFactoryImpl());
      
      assertEquals(3, bindings.size());
      
      assertEquivalent(bindings.get(0), binding1);
      
      assertEquivalent(bindings.get(1), binding3);
      
      assertEquivalent(bindings.get(2), binding4);
      
      
      pm.deleteBinding(binding3);
      
      pm.deleteBinding(binding4);
      
      pm.deleteBinding(binding1);
      
      bindings = pm.loadBindings(new QueueFactoryImpl());
      
      assertEquals(0, bindings.size());                          
   }  
   
   public void testLoadBindings() throws Exception
   {
      Queue queue1 = new QueueImpl(1, "queue1", null, false, true, false, -1);
      
      Queue queue2 = new QueueImpl(2, "queue2", null, false, true, false, -1);
            
      Queue queue3 = new QueueImpl(3, "queue3", null, false, true, false, -1);
      
      Queue queue4 = new QueueImpl(4, "queue4", null, false, true, false, -1);
      
      String condition1 = "queue.condition1";
      
      Binding binding1 = new BindingImpl(1, condition1, queue1);
      
      String condition2 = "queue.condition2";
      
      Binding binding2 = new BindingImpl(1, condition2, queue2);
      
      String condition3 = "queue.condition3";
      
      Binding binding3 = new BindingImpl(3, condition3, queue3);
      
      //same condition
      String condition4 = "queue.condition3";
      
      Binding binding4 = new BindingImpl(3, condition4, queue4);
                        
      pm.addBinding(binding1);
      
      pm.addBinding(binding2);
      
      pm.addBinding(binding3);
      
      pm.addBinding(binding4);
            
      final int numMessages = 10;            
               
      List<Message> msgs = new ArrayList<Message>();
      
      for (int i = 0; i < numMessages; i++)
      {
         Message msg = this.generateMessage(i);
         
         msgs.add(msg);
         
         msg.createReference(queue1);
         
         msg.createReference(queue2);
         
         msg.createReference(queue3);
         
         msg.createReference(queue4);
         
         pm.addMessage(msg);   
      }         
      
      List<Binding> bindings = pm.loadBindings(new QueueFactoryImpl());
      
      assertEquals(4, bindings.size());
      
      assertEquivalent(bindings.get(0), binding1);
      
      assertEquivalent(bindings.get(1), binding2);
      
      assertEquivalent(bindings.get(2), binding3);
      
      assertEquivalent(bindings.get(3), binding4);
      
      checkQueue(bindings.get(0).getQueue(), msgs);
      
      checkQueue(bindings.get(1).getQueue(), msgs);
      
      checkQueue(bindings.get(2).getQueue(), msgs);
      
      checkQueue(bindings.get(3).getQueue(), msgs);
   }  
   
   
   public void testGetInDoubtXids() throws Exception
   {
      Queue queue = createQueue(67);
      
      Message message1 = createMessageWithRefs(1, queue);
      
      List<Message> msgs = new ArrayList<Message>();
      
      msgs.add(message1);
      
      Xid xid1 = generateXid();
      
      pm.prepareTransaction(xid1, msgs, null);    
      
      pm.setInRecoveryMode(true);
      
      List<Xid> xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(1, xids.size());
      
      assertEquals(xid1, xids.get(0));
      
      
      
      Message message2 = createMessageWithRefs(2, queue);
      
      msgs.clear();
      
      msgs.add(message2);
      
      Xid xid2 = generateXid();
      
      pm.prepareTransaction(xid2, msgs, null);    
      
      xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(2, xids.size());
      
      assertTrue(xids.contains(xid1));
      
      assertTrue(xids.contains(xid2));
      
      
      pm.commitPreparedTransaction(xid1);
      
      pm.commitPreparedTransaction(xid2);
      
      xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(0, xids.size());            
   }
   
   public void testGetInDoubtXidsWithRestart() throws Exception
   {
      Queue queue = createQueue(67);
      
      Message message1 = createMessageWithRefs(1, queue);
      
      List<Message> msgs = new ArrayList<Message>();
      
      msgs.add(message1);
      
      Xid xid1 = generateXid();
      
      pm.prepareTransaction(xid1, msgs, null);   
      
      pm.setInRecoveryMode(true);
      
      List<Xid> xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(1, xids.size());
      
      assertEquals(xid1, xids.get(0));
      
      
      
      Message message2 = createMessageWithRefs(2, queue);
      
      msgs.clear();
      
      msgs.add(message2);
      
      Xid xid2 = generateXid();
      
      pm.prepareTransaction(xid2, msgs, null);    
      
      xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(2, xids.size());
      
      assertTrue(xids.contains(xid1));
      
      assertTrue(xids.contains(xid2));
      
      pm.getEnvironment().stop();
            
      pm.stop();
      
      pm.getEnvironment().start();
      
      pm.start();
      
      pm.setInRecoveryMode(true);
      
      xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(2, xids.size());
      
      assertTrue(xids.contains(xid1));
      
      assertTrue(xids.contains(xid2));
      
      
      pm.commitPreparedTransaction(xid1);
      
      pm.commitPreparedTransaction(xid2);
      
      xids = pm.getInDoubtXids();
      
      assertNotNull(xids);
      
      assertEquals(0, xids.size());            
   }
   
   public void testSetGetRecoveryMode() throws Exception
   {
      assertFalse(pm.isInRecoveryMode());
                 
      try
      {
         pm.getInDoubtXids();
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      pm.setInRecoveryMode(true);
      
      assertTrue(pm.isInRecoveryMode());
      
      pm.getInDoubtXids();
      
      pm.setInRecoveryMode(false);
      
      assertFalse(pm.isInRecoveryMode());
   }
   
   // Private --------------------------------------------------------------------
   
   private void checkQueue(Queue queue, List<Message> messages)
   {
      assertEquals(messages.size(), queue.getMessageCount());
      
      List<MessageReference> refs = queue.list(null);
      
      int i = 0;
      for (MessageReference ref: refs)
      {
         assertEquivalent(messages.get(i++), ref.getMessage(), false);
      } 
   }
   
   private void assertEquivalent(Binding b1, Binding b2)
   {
      assertEquals(b1.getNodeID(), b2.getNodeID());
      
      assertEquals(b1.getAddress(), b2.getAddress());
      
      Queue queue1 = b1.getQueue();
      
      Queue queue2 = b2.getQueue();
      
      assertEquals(queue1.getPersistenceID(), queue2.getPersistenceID());
      
      assertEquals(queue1.getName(), queue2.getName());
      
      assertEquals(queue1.isClustered(), queue2.isClustered());
      
      assertEquals(queue1.isDurable(), queue2.isDurable());
      
      assertEquals(queue1.isTemporary(), queue2.isTemporary());
   }
   
   private Message extractMessage(Map<Long, Queue> queues, long id, byte[] msgBytes, byte[] refBytes) throws Exception
   {
      ByteBuffer buffer = ByteBuffer.wrap(msgBytes);
      
      int type = buffer.getInt();
      
      long expiration = buffer.getLong();
      
      long timestamp = buffer.getLong();
      
      byte priority = buffer.get();
      
      int headerSize = buffer.getInt();
      
      byte[] headers = new byte[headerSize];
      
      buffer.get(headers);
      
      int payloadSize = buffer.getInt();
      
      byte[] payload = null;
      
      if (payloadSize != 0)
      {
         payload = new byte[payloadSize];
         
         buffer.get(payload);
      }
      
      Message message = new MessageImpl(id, type, true, expiration, timestamp, priority,
                                        headers, payload);
      
      buffer = ByteBuffer.wrap(refBytes);
      
      while (buffer.hasRemaining())
      {
         long queueID = buffer.getLong();
         
         int deliveryCount = buffer.getInt();
         
         long scheduledDeliveryTime = buffer.getLong();
         
         MessageReference reference = message.createReference(queues.get(queueID));
         
         reference.setDeliveryCount(deliveryCount);
         
         reference.setScheduledDeliveryTime(scheduledDeliveryTime);
      } 
      
      return message;
   }
   
   private void assertMessageInStore(Message m, Queue queue) throws Exception
   {
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      BDBJEDatabase refDB = bdb.getDatabase(BDBJEPersistenceManager.REFERENCE_DB_NAME);
      
      byte[] msgBytes = msgDB.get(m.getMessageID());
      
      assertNotNull(msgBytes);
      
      byte[] refBytes = refDB.get(m.getMessageID());
      
      assertNotNull(refBytes);
      
      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      
      queues.put(queue.getPersistenceID(), queue);
      
      Message m2 = extractMessage(queues, m.getMessageID(), msgBytes, refBytes);
       
      assertEquivalent(m, m2);               
   }
   
   private void assertNumMessagesInStore(int num) throws Exception
   {
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      assertEquals(num, msgDB.size());                    
   }
   
   private void assertMessageNotInStore(Message m) throws Exception
   {
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      BDBJEDatabase refDB = bdb.getDatabase(BDBJEPersistenceManager.REFERENCE_DB_NAME);
      
      
      byte[] msgBytes = msgDB.get(m.getMessageID());
      
      assertNull(msgBytes);
      
      byte[] refBytes = refDB.get(m.getMessageID());
      
      assertNull(refBytes);         
   }
   
   private void assertStoreEmpty() throws Exception
   {
      BDBJEDatabase msgDB = bdb.getDatabase(BDBJEPersistenceManager.MESSAGE_DB_NAME);
      
      BDBJEDatabase refDB = bdb.getDatabase(BDBJEPersistenceManager.REFERENCE_DB_NAME);
      
      assertEquals(0, msgDB.size());
      
      assertEquals(0, refDB.size());
   }
   
   private Message createMessageWithRefs(long id, Queue queue)
   {
      Message m = generateMessage(id);
      
      m.createReference(queue);
      
      m.createReference(queue);
      
      m.createReference(queue);
      
      m.createReference(queue);
      
      return m;
   }
   

   // Inner classes ---------------------------------------------------------------
        
}
