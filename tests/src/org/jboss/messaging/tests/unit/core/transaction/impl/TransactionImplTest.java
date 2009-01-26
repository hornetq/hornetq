/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.transaction.impl;

import static org.jboss.messaging.tests.util.RandomUtil.randomXid;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A TransactionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransactionImplTest extends UnitTestCase
{
	private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

   private final HierarchicalRepository<QueueSettings> queueSettings = 
   	new HierarchicalObjectRepository<QueueSettings>();
	
   protected void setUp() throws Exception
   {
   	super.setUp();
   	
   	queueSettings.setDefault(new QueueSettings());
   }
   
   public void testFoo()
   {      
   }
   
//   public void testNonXAConstructor() throws Exception
//   {
//   	StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
//      
//      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
//      
//      final long txID = 123L;
//      
//      EasyMock.expect(sm.generateUniqueID()).andReturn(txID);
//   	
//      EasyMock.replay(sm);
//      
//   	Transaction tx = new TransactionImpl(sm, po);
//   	
//   	EasyMock.verify(sm);
//   	
//   	assertEquals(txID, tx.getID());
//   	
//   	assertNull(tx.getXid());
//   	
//   	assertEquals(0, tx.getOperationsCount());
//   }
//         
//   public void testXAConstructor() throws Exception
//   {
//   	StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
//      
//      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
//      
//      final long txID = 123L;
//      
//      EasyMock.expect(sm.generateUniqueID()).andReturn(txID);
//   	
//      EasyMock.replay(sm);
//      
//      Xid xid = randomXid();
//      
//   	Transaction tx = new TransactionImpl(xid, sm, po);
//   	
//   	EasyMock.verify(sm);
//   	
//   	assertEquals(txID, tx.getID());
//   	
//   	assertEquals(xid, tx.getXid());
//   	
//   	assertEquals(0, tx.getOperationsCount());
//   }
//   
//   public void testState() throws Exception
//   {
//      Transaction tx = createTransaction();
//      
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      tx.suspend();
//      
//      assertEquals(Transaction.State.SUSPENDED, tx.getState());
//      
//      tx.resume();
//      
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      tx.commit();
//      
//      assertEquals(Transaction.State.COMMITTED, tx.getState());
//      
//      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
//      
//      try
//      {
//      	tx.rollback(repos);
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.commit();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.prepare();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.suspend();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.resume();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      tx = createTransaction();
//      
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      tx.rollback(repos);
//      
//      try
//      {
//      	tx.rollback(repos);
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.commit();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.prepare();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.suspend();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.resume();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK      	      	      	
//      }
//      
//      tx = createTransaction();
//      
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      try
//      {
//      	tx.prepare();
//      	
//      	fail("Should throw exception");
//      }
//      catch (Exception e)
//      {
//      	//OK
//      }
//      
//      
//      CreatedTrans resultTrans = createTransactionXA();
//      tx = resultTrans.tx;
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      EasyMock.reset(resultTrans.sm);
//      
//      resultTrans.sm.prepare(EasyMock.eq(resultTrans.txId), EasyMock.eq(resultTrans.xid));
//      resultTrans.sm.commit(resultTrans.txId);
//      
//      EasyMock.replay(resultTrans.sm);
//
//      tx.prepare();
//      
//      tx.commit();
//      
//      try
//      {
//      	tx.rollback(repos);
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.commit();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.prepare();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.suspend();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.resume();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      EasyMock.verify(resultTrans.sm);
//
//      resultTrans =  createTransactionXA();
//      
//      tx = resultTrans.tx;
//      
//      
//      EasyMock.reset(resultTrans.sm);
//      
//      resultTrans.sm.prepare(resultTrans.txId, resultTrans.xid);
//      resultTrans.sm.rollback(resultTrans.txId);
//      
//      EasyMock.replay(resultTrans.sm);
//      
//      assertEquals(Transaction.State.ACTIVE, tx.getState());
//      
//      tx.prepare();
//      
//      tx.rollback(repos);
//      
//      try
//      {
//      	tx.rollback(repos);
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.commit();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.prepare();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.suspend();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      try
//      {
//      	tx.resume();
//      	
//      	fail("Should throw exception");
//      }
//      catch (IllegalStateException e)
//      {
//      	//OK
//      }
//      
//      
//      EasyMock.verify(resultTrans.sm);
//      
//   }
//   
//   public void testSendCommit() throws Exception
//   {
//      //Durable queue
//      Queue queue1 = new QueueImpl(12, new SimpleString("queue1"), null, false, true, -1, scheduledExecutor);
//      
//      //Durable queue
//      Queue queue2 = new QueueImpl(34, new SimpleString("queue2"), null, false, true, -1, scheduledExecutor);
//      
//      //Non durable queue
//      Queue queue3 = new QueueImpl(65, new SimpleString("queue3"), null, false, false, -1, scheduledExecutor);
//      
//      //Durable message to send
//      
//      ServerMessage message1 = this.generateMessage(1);
//      
//      // Non durable message to send
//      
//      ServerMessage message2 = this.generateMessage(2);
//      
//      message2.setDurable(false);
//      
//      
//      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
//      
//      PostOffice po= EasyMock.createStrictMock(PostOffice.class);
//      
//      final long txID = 123;
//      
//      EasyMock.expect(sm.generateTransactionID()).andReturn(txID);
//      
//      EasyMock.replay(sm);
//            
//      Transaction tx = new TransactionImpl(sm, po);
//      
//      assertTrue(tx.hasConsumers());
//      assertFalse(tx.isContainsPersistent());
//
//      EasyMock.verify(sm);
//      
//      EasyMock.reset(sm);
//      
//      final SimpleString address1 = new SimpleString("topic1");
//      
//      //Expect:
//      
//      MessageReference ref5 = message1.createReference(queue1);
//      MessageReference ref6 = message1.createReference(queue2);
//      List<MessageReference> message1Refs = new ArrayList<MessageReference>();
//      message1Refs.add(ref5);
//      message1Refs.add(ref6);
//      
//      EasyMock.expect(po.route(address1, message1)).andReturn(message1Refs);
//      
//      sm.storeMessageTransactional(txID, address1, message1);
//      
//      EasyMock.replay(po);
//      
//      EasyMock.replay(sm);
//      
//      tx.addMessage(address1, message1);
//      
//      assertFalse(tx.hasConsumers());
//      assertTrue(tx.isContainsPersistent());
//      
//         
//      EasyMock.verify(po);
//      
//      EasyMock.verify(sm);
//      
//      EasyMock.reset(po);
//      
//      EasyMock.reset(sm);
//      
//                       
//      //Expect:
//      
//      final SimpleString address2 = new SimpleString("queue3");
//      
//      MessageReference ref7 = message2.createReference(queue3);
//      List<MessageReference> message2Refs = new ArrayList<MessageReference>();
//      message2Refs.add(ref7);
//
//      EasyMock.expect(po.route(address2, message2)).andReturn(message1Refs);
//      
//      EasyMock.replay(po);
//      
//      EasyMock.replay(sm);
//      
//      tx.addMessage(address2, message2);
//      
//      EasyMock.verify(po);
//      
//      EasyMock.verify(sm);
//      
//      EasyMock.reset(po);
//      
//      EasyMock.reset(sm);
//      
//      //Expect :
//      
//      sm.commit(txID);
//      
//      EasyMock.replay(sm);
//      
//      tx.commit();
//      
//      EasyMock.verify(sm);
//      
//      //TODO test messages are routed and refs count reduced
//   }
//   
   
//   
//   public void testAckCommit() throws Exception
//   {
//      
//      PagingManager pagingManager = EasyMock.createStrictMock(PagingManager.class);
//      PostOffice postOffice = EasyMock.createNiceMock(PostOffice.class);
//      PagingStore pagingStore = EasyMock.createNiceMock(PagingStore.class);
//      
//      EasyMock.expect(pagingManager.getPageStore((SimpleString)EasyMock.anyObject())).andStubReturn(pagingStore);
//      EasyMock.expect(postOffice.getPagingManager()).andStubReturn(pagingManager);
//      
//      EasyMock.replay(pagingManager, postOffice);
//      
//      //Durable queue
//      Queue queue1 = new QueueImpl(12, new SimpleString("queue1"), null, false, true, false, scheduledExecutor, postOffice, null);
//      
//      //Durable queue
//      Queue queue2 = new QueueImpl(34, new SimpleString("queue2"), null, false, true, false, scheduledExecutor, postOffice, null);
//      
//      //Non durable queue
//      Queue queue3 = new QueueImpl(65, new SimpleString("queue3"), null, false, false, false, scheduledExecutor, postOffice, null);
//      
//      //Some refs to ack
//      
//      ServerMessage message1 = this.generateMessage(12);
//      
//      MessageReference ref1 = message1.createReference(queue1);
//      
//      MessageReference ref2 = message1.createReference(queue2);
//      
//      MessageReference ref3 = message1.createReference(queue3);
//      
//      
//      //Non durable message to ack
//      ServerMessage message2 = this.generateMessage(23);
//      
//      message2.setDurable(false);
//            
//      MessageReference ref4 = message2.createReference(queue1);
//      
//         
//      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
//      
//      final long txID = 123;
//      
//      EasyMock.expect(sm.generateUniqueID()).andReturn(txID);
//      
//      EasyMock.reset(postOffice, pagingManager, pagingStore);
//      
//      EasyMock.replay(sm, postOffice, pagingManager, pagingStore);
//            
//      Transaction tx = new TransactionImpl(sm, postOffice);
//      
//      assertFalse(tx.isContainsPersistent());
//            
//      EasyMock.verify(sm, postOffice, pagingManager, pagingStore);
//      
//      EasyMock.reset(sm, postOffice, pagingManager, pagingStore);
//      
//      //Expect:
//      
//      sm.storeAcknowledgeTransactional(txID, queue1.getPersistenceID(), message1.getMessageID());
//      sm.deleteMessageTransactional(txID, queue2.getPersistenceID(), message1.getMessageID());
//      
//      EasyMock.replay(sm, postOffice, pagingManager, pagingStore);
//      
//      tx.addAcknowledgement(ref3);
//      
//      assertFalse(tx.isContainsPersistent());
//      
//      tx.addAcknowledgement(ref1);
//      
//      assertTrue(tx.isContainsPersistent());
//      
//      tx.addAcknowledgement(ref2);
//      
//      assertTrue(tx.isContainsPersistent());
//      
//      
//      assertEquals(3, tx.getAcknowledgementsCount());
//      
//      EasyMock.verify(sm, postOffice, pagingManager, pagingStore);
//      
//      EasyMock.reset(sm, postOffice, pagingManager, pagingStore);
//      
//      //Expect:
//      
//      //Nothing
//      
//      EasyMock.replay(sm, postOffice, pagingManager, pagingStore);
//      
//      tx.addAcknowledgement(ref4);
//      
//      assertEquals(4, tx.getAcknowledgementsCount());
//      
//      EasyMock.verify(sm, postOffice, pagingManager, pagingStore);
//      
//      EasyMock.reset(sm, postOffice, pagingManager, pagingStore);
//      
//      //Expect:
//      
////      postOffice.deliver((List<MessageReference>)EasyMock.anyObject());
////      
////      EasyMock.expectLastCall().anyTimes();
////      
//      sm.commit(txID);
//
//      EasyMock.expect(pagingManager.getPageStore((SimpleString)EasyMock.anyObject())).andStubReturn(pagingStore);
//      EasyMock.expect(postOffice.getPagingManager()).andStubReturn(pagingManager);
// 
//      EasyMock.replay(sm, postOffice, pagingManager, pagingStore);
//      
//      tx.commit();
//      
//      EasyMock.verify(sm, postOffice, pagingManager, pagingStore);
//      
//      EasyMock.reset(sm, postOffice, pagingManager, pagingStore);            
//      
//      //TODO test messages are routed and refs count reduced
//   }
   
   // Private -------------------------------------------------------------------------
   
   private Transaction createTransaction() throws Exception
   {
   	StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      
      PostOffice po = EasyMock.createNiceMock(PostOffice.class);
      
      final long txID = 123L;
      
      EasyMock.expect(sm.generateUniqueID()).andReturn(txID);
   	
      EasyMock.replay(sm, po);
      
      Transaction tx = new TransactionImpl(sm);
      
      EasyMock.verify(sm, po);
      
      EasyMock.reset(po);
      
//      po.deliver((List<MessageReference>)EasyMock.anyObject());
//      
//      EasyMock.expectLastCall().anyTimes();
      
      EasyMock.replay(po);
      
      return tx;
   }
   
   private CreatedTrans createTransactionXA() throws Exception
   {
      CreatedTrans trans = new CreatedTrans();
      
   	trans.sm = EasyMock.createStrictMock(StorageManager.class);
      
      trans.po = EasyMock.createMock(PostOffice.class);
      
      EasyMock.expect(trans.po.getPagingManager()).andStubReturn(null);
      
      trans.txId = 123L;
   	
      trans.xid = randomXid();
      
      EasyMock.expect(trans.sm.generateUniqueID()).andReturn(trans.txId);

      EasyMock.replay(trans.sm, trans.po);

      trans.tx = new TransactionImpl(trans.xid, trans.sm, trans.po);
      
      EasyMock.verify(trans.sm, trans.po);
      
      EasyMock.reset(trans.sm, trans.po);
      
      return trans;
   }
  
   
   // Inner classes -----------------------------------------------------------------------
   
   
   class CreatedTrans
   {
      TransactionImpl tx;
      PostOffice po;
      StorageManager sm;
      Xid xid;
      long txId;
   }

}
