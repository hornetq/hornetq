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
package org.jboss.messaging.newcore.impl.bdbje.test.unit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.newcore.impl.MessageImpl;
import org.jboss.messaging.newcore.impl.QueueImpl;
import org.jboss.messaging.newcore.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.newcore.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.newcore.impl.bdbje.BDBJEPersistenceManager;
import org.jboss.messaging.newcore.impl.bdbje.test.unit.fakes.FakeBDBJEEnvironment;
import org.jboss.messaging.newcore.intf.Message;
import org.jboss.messaging.newcore.intf.MessageReference;
import org.jboss.messaging.newcore.intf.Queue;
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
      
      pm = new BDBJEPersistenceManager(bdb, ENV_DIR);
      
      pm.start();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      pm.stop();
   }
   
   // The tests ----------------------------------------------------------------
              
   public void testAddMessage() throws Exception
   {      
      Queue queue = new QueueImpl(67);
            
      Message m = createMessageWithRefs(1, queue);
      
      pm.addMessage(m);
      
      assertMessageInStore(m, queue);   
   }
         
   public void testDeleteReference() throws Exception
   {            
      Queue queue = new QueueImpl(67);
      
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
   }
   
   public void testCommitTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = new QueueImpl(67);
            
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
      pm.commitTransaction(null, null);
      
   }     
   
   public void testPrepareAndCommitTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = new QueueImpl(67);
            
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
      pm.prepareTransaction(xid, null, null);
      pm.commitPreparedTransaction(xid);
      
   }     
   
   public void testPrepareAndUnprepareTransaction() throws Exception
   {      
      List<Message> msgs = new ArrayList<Message>();
          
      Queue queue = new QueueImpl(67);
            
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
      Queue queue = new QueueImpl(67);
      
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
         Queue queue = new QueueImpl(i);
         
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
   
   public void testLoadQueues() throws Exception
   {
      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      
      final int numQueues = 10;
      
      final int numMessages = 10;
      
      for (int i = 0; i < numQueues; i++)
      {
         Queue queue = new QueueImpl(i);
         
         queues.put(queue.getID(), queue);
      }
         
      List<Message> msgs = new ArrayList<Message>();
      
      for (int i = 0; i < numMessages; i++)
      {
         Message msg = this.generateMessage(i);
         
         msgs.add(msg);
         
         for (long j = 0; j < numQueues; j++)
         {
            Queue queue = queues.get(j);
            
            msg.createReference(queue);
         }
         
         pm.addMessage(msg);   
      }         
      
      
      pm.loadQueues(queues);
      
      for (Queue queue: queues.values())
      {
         assertEquals(numMessages, queue.getMessageCount());
         
         List<MessageReference> refs = queue.list(null);
         
         int i = 0;
         for (MessageReference ref: refs)
         {
            this.assertEquivalent(msgs.get(i++), ref.getMessage());
         }
      }            
   }  
   
   public void testGetInDoubtXids() throws Exception
   {
      Queue queue = new QueueImpl(12);
      
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
      Queue queue = new QueueImpl(12);
      
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
      
      pm.stop();
      
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
      
      queues.put(queue.getID(), queue);
      
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
