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
package org.jboss.test.messaging.core.paging;

import java.util.List;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.ChannelState;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.LockMap;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * A PagingTest_P_2PC_Recoverable.
 * 
 * Persistent messages, 2pc , recoverable
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * SingleChannel_P_2PC.java,v 1.1 2006/03/22 10:23:35 timfox Exp
 */
public class SingleChannel_P_2PCTest extends PagingStateTestBase
{
   public SingleChannel_P_2PCTest(String name)
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
   
   public void test1() throws Throwable
   {
      Channel queue = new Queue(1, ms, pm, true, 100, 20, 10);
      
      ChannelState state = new ChannelState(queue, pm, true, true, 100, 20, 10);
                       
      Message[] msgs = new Message[241];
      
      MessageReference[] refs = new MessageReference[241];
  
      //Send 99
      
      Transaction tx = createXATx();
      for (int i = 0; i < 99; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i], tx);
      }
      tx.prepare();
      tx.commit();
      
      //verify no unloaded refs in storage
            
      List refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //verify 99 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(99, refIds.size());
      assertSameIds(refIds, refs, 0, 98);
      
      //Verify 99 msgs in storage
      List msgIds = getMessageIds();
      assertEquals(99, msgIds.size());
      assertSameIds(msgIds, refs, 0, 98);
      
      //Verify 99 msgs in store
      assertEquals(99, ms.size());
      
      //Verify 99 refs in queue
      assertEquals(99, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify not paging
      assertFalse(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      //Send one more ref
      
      tx = createXATx();
      msgs[99] = MessageFactory.createCoreMessage(99, true, null);
      refs[99] = ms.reference(msgs[99]);
      state.addReference(refs[99], tx);
      tx.prepare();
      tx.commit();
      
      //verify no unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //verify 100 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(100, refIds.size());
      assertSameIds(refIds, refs, 0, 99);
      
      //Verify 100 msgs in storage
      msgIds = getMessageIds();
      assertEquals(100, msgIds.size());
      assertSameIds(msgIds, refs, 0, 99);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
            
      //Verify paging
      assertTrue(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      //Send 9 more
      
      tx = createXATx();
      for (int i = 100; i < 109; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);         
      }
      tx.prepare();
      tx.commit();
      
      //verify no unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //verify 109 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(109, refIds.size());
      assertSameIds(refIds, refs, 0, 108);      
      
      //Verify 100 msgs in storage
      msgIds = getMessageIds();
      assertEquals(109, msgIds.size());
      assertSameIds(msgIds, refs, 0, 108);
      
      //Verify 109 msgs in store
      assertEquals(109, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 9 refs in downcache
      assertEquals(9, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Send one more ref - should clear the down cache
      
      tx = createXATx();
      msgs[109] = MessageFactory.createCoreMessage(109, true, null);
      refs[109] = ms.reference(msgs[109]);
      state.addReference(refs[109]);
      tx.prepare();
      tx.commit();
      
      //verify 10 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 100, 109);
      
      //verify 110 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(110, refIds.size());
      assertSameIds(refIds, refs, 0, 109);
      
      //Verify 110 msgs in storage
      msgIds = getMessageIds();
      assertEquals(110, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 109);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      //Send one more ref
      tx = createXATx();
      msgs[110] = MessageFactory.createCoreMessage(110, true, null);
      refs[110] = ms.reference(msgs[110]);
      state.addReference(refs[110]);
      tx.prepare();
      tx.commit();
      
      //verify 10 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 100, 109);
      
      //verify 111 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(111, refIds.size());
      assertSameIds(refIds, refs, 0, 110);
      
      //Verify 111 msgs in storage
      msgIds = getMessageIds();
      assertEquals(111, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 110);
      
      //Verify 101 msgs in store
      assertEquals(101, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      //Send 9 more refs
      tx = createXATx();
      for (int i = 111; i < 120; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);         
      }     
      tx.prepare();
      tx.commit();
      
      //verify 20 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 100, 119);
      
      //verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size());
      assertSameIds(refIds, refs, 0, 119);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 119);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      //    Send 100 more refs then roll back
      tx = this.createXATx();
      for (int i = 200; i < 300; i++)
      {
         Message m = MessageFactory.createCoreMessage(i, true, null);
         MessageReference ref = ms.reference(m);
         state.addReference(ref, tx);         
      }  
      tx.prepare();
      tx.rollback();
      
      
      //Send 10 more refs
      tx = createXATx();
      for (int i = 120; i < 130; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);         
      }  
      tx.prepare();
      tx.commit();
      
      //verify 30 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(30, refIds.size());
      assertSameIds(refIds, refs, 100, 129);
      
      //verify 130 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(130, refIds.size());
      assertSameIds(refIds, refs, 0, 129);
      
      //Verify 130 msgs in storage
      msgIds = getMessageIds();
      assertEquals(130, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 129);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Send 10 more refs
      tx = createXATx();;
      for (int i = 130; i < 140; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);         
      }  
      tx.prepare();
      tx.commit();
      
      //verify 40 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //verify 140 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(140, refIds.size());
      assertSameIds(refIds, refs, 0, 139);
      
      //Verify 140 msgs in storage
      msgIds = getMessageIds();
      assertEquals(140, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 139);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());  

      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      
      //Send one more ref
      tx = createXATx();
      msgs[140] = MessageFactory.createCoreMessage(140, true, null);
      refs[140] = ms.reference(msgs[140]);
      state.addReference(refs[140]);
      tx.prepare();
      tx.commit();
      
      //verify 40 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //verify 141 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(141, refIds.size());
      assertSameIds(refIds, refs, 0, 140);
      
      //Verify 141 msgs in storage
      msgIds = getMessageIds();
      assertEquals(141, msgIds.size()); 
      assertSameIds(msgIds, refs, 0, 140);
      
      //Verify 101 msgs in store
      assertEquals(101, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());  
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
                            
      //Consume 1
      int consumeCount = 0;
      
      consumeIn2PCTx(queue, state, consumeCount, refs, 1);
      consumeCount++;

      //verify 40 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //verify 140 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(140, refIds.size());
      assertSameIds(refIds, refs, 1, 140);      
      
      //Verify 140 msgs in storage
      msgIds = getMessageIds();
      assertEquals(140, msgIds.size()); 
      assertSameIds(msgIds, refs, 1, 140);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 99 refs in queue
      assertEquals(99, state.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Now we should have 99 refs in memory, 140 refs in storage, and 1 in down cache, 99 msgs in memory
      
      //Consume 18 more
     
      consumeIn2PCTx(queue, state, consumeCount, refs, 18);
      consumeCount += 18;
      
      
      //verify 40 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //Verify 122 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(122, refIds.size()); 
      assertSameIds(refIds, refs, 19, 140);
      
      //Verify 122 msgs in storage
      msgIds = getMessageIds();
      assertEquals(122, msgIds.size()); 
      assertSameIds(msgIds, refs, 19, 140);
      
      //Verify 82 msgs in store
      assertEquals(82, ms.size());
      
      //Verify 81 refs in queue
      assertEquals(81, state.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Consume one more
      
      consumeIn2PCTx(queue, state, consumeCount, refs, 1);
      consumeCount++;
      
      //This should force a load of 20 and flush the downcache
      
      //verify 21 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(21, refIds.size());
      assertSameIds(refIds, refs, 120, 140);
      
      //Verify 121 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(121, refIds.size()); 
      assertSameIds(refIds, refs, 20, 140);
      
      //Verify 121 msgs in storage
      msgIds = getMessageIds();
      assertEquals(121, msgIds.size()); 
      assertSameIds(msgIds, refs, 20, 140);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Consume 20 more
      
      consumeIn2PCTx(queue, state, consumeCount, refs, 20);
      consumeCount += 20;
      
      //verify 1 unloaded ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(1, refIds.size());
      assertSameIds(refIds, refs, 140, 140);
      
      //Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(101, refIds.size()); 
      assertSameIds(refIds, refs, 40, 140);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(101, msgIds.size()); 
      assertSameIds(msgIds, refs, 40, 140);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      
      //Consume 1 more
      
      consumeIn2PCTx(queue, state, consumeCount, refs, 1);
      consumeCount ++;
      
      //verify 0 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 100 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(100, refIds.size()); 
      assertSameIds(refIds, refs, 41, 140);
      
      //Verify 100 msgs in storage
      msgIds = getMessageIds();
      assertEquals(100, msgIds.size()); 
      assertSameIds(msgIds, refs, 41, 140); 
      
      //Verify 81 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 81 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      
      //Consume 20 more
      
      consumeIn2PCTx(queue, state, consumeCount, refs, 20);
      consumeCount += 20;
      
      //verify 0 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 80 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(80, refIds.size()); 
      assertSameIds(refIds, refs, 61, 140);
      
      //Verify 80 msgs in storage
      msgIds = getMessageIds();
      assertEquals(80, msgIds.size()); 
      assertSameIds(msgIds, refs, 61, 140); 
      
      //Verify 80 msgs in store
      assertEquals(80, ms.size());
      
      //Verify 80 refs in queue
      assertEquals(80, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify not paging
      assertFalse(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      //Consumer 60 more
      
            
      consumeIn2PCTx(queue, state, consumeCount, refs, 60);
      consumeCount += 60;
      
      //verify 0 unloaded refs in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 20 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(20, refIds.size()); 
      assertSameIds(refIds, refs, 121, 140);
      
      //Verify 20 msgs in storage
      msgIds = getMessageIds();
      assertEquals(20, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 140);  
      
      //Verify 20 msgs in store
      assertEquals(20, ms.size());
      
      //Verify 20 refs in queue
      assertEquals(20, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify not paging
      assertFalse(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
            
      
      
      //Add 20 more messages
      tx = createXATx();
      for (int i = 141; i < 161; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);
      }
      tx.prepare();
      tx.commit();
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 40 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(40, refIds.size()); 
      assertSameIds(refIds, refs, 121, 160);
      
      //Verify 40 msgs in storage
      msgIds = getMessageIds();
      assertEquals(40, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 160);
      
      //Verify 40 msgs in store
      assertEquals(40, ms.size());
      
      //Verify 40 refs in queue
      assertEquals(40, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify not paging
      assertFalse(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      
      //Add 20 more messages
      tx = createXATx();
      for (int i = 161; i < 181; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);
      }
      tx.prepare();
      tx.commit();
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 60 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(60, refIds.size()); 
      assertSameIds(refIds, refs, 121, 180);
      
      //Verify 60 msgs in storage
      msgIds = getMessageIds();
      assertEquals(60, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 180); 
      
      //Verify 60 msgs in store
      assertEquals(60, ms.size());
      
      //Verify 60 refs in queue
      assertEquals(60, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify not paging
      assertFalse(state.isPaging());
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      //Add 60 more messages
      tx = createXATx();
      for (int i = 181; i < 241; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         refs[i] = ms.reference(msgs[i]);
         state.addReference(refs[i]);
      }
      tx.prepare();
      tx.commit();
      
      //verify 20 unloaded ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 221, 240);
      
      // Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size()); 
      assertSameIds(refIds, refs, 121, 240);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 240);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, state.downCacheCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, state.memoryDeliveryCount());;      
      
      
       
      //test cancellation
      
      //remove 20 but don't ack them yet
      //this should cause a load to be triggered
      
      SimpleDelivery[] dels = new SimpleDelivery[20];
      for (int i = 0; i < 20; i++)
      {
         MessageReference ref = state.removeFirstInMemory();
         assertNotNull(ref);
         assertEquals(refs[i + consumeCount].getMessageID(), ref.getMessageID());
         dels[i] = new SimpleDelivery(queue, ref, false);
         state.addDelivery(dels[i]);
      }
      
      //verify 0 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      // Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size()); 
      assertSameIds(refIds, refs, 121, 240);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 240);
  
      //Verify 120 msgs in store
      assertEquals(120, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify 20 deliveries
      assertEquals(20, state.memoryDeliveryCount());;      
      
      
       
      //Cancel last 7
      for (int i = 19; i > 12; i--)
      {
         state.cancelDelivery(dels[i]);   
      }
      
      //This should cause the refs corresponding to the deliveries to go the front of the in memory quuee
      //and the oldest refs in memory evicted off the end into the down cache
      
      //verify 0 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      // Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size()); 
      assertSameIds(refIds, refs, 121, 240);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 240);      
  
      //Verify 120 msgs in store
      assertEquals(120, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify 13 deliveries
      assertEquals(13, state.memoryDeliveryCount());;      
      
      
   
      //Cancel 3 more
      
      for (int i = 12; i > 9; i--)
      {
         state.cancelDelivery(dels[i]);
      }
      
      //This should cause the down cache to be flushed
      
      //verify 10 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 231, 240);
      
      // Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size()); 
      assertSameIds(refIds, refs, 121, 240);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 240);      
            
      //Verify 110 msgs in store
      assertEquals(110, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify 10 deliveries
      assertEquals(10, state.memoryDeliveryCount());;      
      
            
      
      //Cancel the last 10
      
      for (int i = 9; i >= 0; i--)
      {
         state.cancelDelivery(dels[i]);
      }
      
      //consumeCount += 20;
      
      //This should cause the down cache to be flushed
      
      //verify 20 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 221, 240);
      
      // Verify 120 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(120, refIds.size()); 
      assertSameIds(refIds, refs, 121, 240);
      
      //Verify 120 msgs in storage
      msgIds = getMessageIds();
      assertEquals(120, msgIds.size()); 
      assertSameIds(msgIds, refs, 121, 240);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, state.memoryRefCount());
      
      //Verify paging
      assertTrue(state.isPaging());      
      
      //Verify 0 deliveries
      assertEquals(0, state.memoryDeliveryCount());;      
      
      


      //Now there should be 120 message left to consume
      
      //Consume 50
      
      consume(queue, state, consumeCount, refs, 50);
      consumeCount += 50;
      
      //verify 0 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());     
      
      // Verify 70 refs in storage
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(70, refIds.size()); 
      assertSameIds(refIds, refs, 171, 240);
      
      //Verify 70 msgs in storage
      msgIds = getMessageIds();
      assertEquals(70, msgIds.size()); 
      assertSameIds(msgIds, refs, 171, 240); 

      //Verify 70 msgs in store
      assertEquals(70, ms.size());
      
      //Verify 70 refs in queue
      assertEquals(70, state.memoryRefCount());  
      
      //Verify not paging
      assertFalse(state.isPaging());      
      
      //Verify 0 deliveries
      assertEquals(0, state.memoryDeliveryCount());
      
      
      
      
      //Consume the rest
      
      consume(queue, state, consumeCount, refs, 70);
      consumeCount += 70;
      
      //verify 0 ref in storage
      
      refIds = getUnloadedReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());     
      
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(0, refIds.size());     
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 

      //Verify 0 msgs in store
      assertEquals(0, ms.size());
      
      //Verify 0 refs in queue
      assertEquals(0, state.memoryRefCount());
      
      //Make sure there are no more refs in state
      
      MessageReference ref = state.removeFirstInMemory();
      
      assertNull(ref);
      
      assertEquals(0, LockMap.instance.getSize());
         
   }
}
