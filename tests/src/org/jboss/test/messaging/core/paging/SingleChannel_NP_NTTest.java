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

import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.SimpleDelivery;
import org.jboss.messaging.util.LockMap;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * 
 * A PagingTest_NP_NT_Recoverable.
 * 
 * Non Persistent messages, non transactional, recoverable
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SingleChannel_NP_NTTest extends PagingStateTestBase
{
   public SingleChannel_NP_NTTest(String name)
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
      MessagingQueue queue = new MessagingQueue(1, "queue1", 1, ms, pm, true, -1, null, 100, 20, 10, false, false);
      queue.activate(); 
      
      Message[] msgs = new Message[241];
      
      MessageReference[] refs = new MessageReference[241];
  
      //Send 99
      
      for (int i = 0; i < 99; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);
         refs[i].releaseMemoryReference();
      }
      
      //verify no refs in storage
            
      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Verify no msgs in storage
      List msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
      
      //Verify 99 msgs in store
      assertEquals(99, ms.size());
      
      //Verify 99 refs in queue
      assertEquals(99, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify not paging
      assertFalse(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      //Send one more ref
      
      msgs[99] = CoreMessageFactory.createCoreMessage(99, false, null);
      refs[99] = ms.reference(msgs[99]);
      queue.handle(null, refs[99], null);
      refs[99].releaseMemoryReference();
      
      //verify no refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Verify no msgs in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
            
      //Verify paging
      assertTrue(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
                  
      //Send 9 more
      
      for (int i = 100; i < 109; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null); 
         refs[i].releaseMemoryReference();
      }
      
      //verify no refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertTrue(refIds.isEmpty());
      
      //Verify no msgs in storage
      msgIds = getMessageIds();
      assertTrue(msgIds.isEmpty());
      
      //Verify 100 msgs in store
      assertEquals(109, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 9 refs in downcache
      assertEquals(9, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Send one more ref - should clear the down cache
      
      msgs[109] = CoreMessageFactory.createCoreMessage(109, false, null);
      refs[109] = ms.reference(msgs[109]);
      queue.handle(null, refs[109], null);
      refs[109].releaseMemoryReference();
      
      //verify 10 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 100, 109);
      
      //Verify 10 msgs in storage
      msgIds = getMessageIds();
      assertEquals(10, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 109);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      //Send one more ref
      
      msgs[110] = CoreMessageFactory.createCoreMessage(110, false, null);
      refs[110] = ms.reference(msgs[110]);
      queue.handle(null, refs[110], null);
      refs[110].releaseMemoryReference();
      
      //verify 10 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 100, 109);
      
      //Verify 10 msgs in storage
      msgIds = getMessageIds();
      assertEquals(10, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 109);
      
      //Verify 101 msgs in store
      assertEquals(101, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      //Send 9 more refs
      
      for (int i = 111; i < 120; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);         
         refs[i].releaseMemoryReference();
      }      
      
      //verify 20 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 100, 119);
      
      //Verify 20 msgs in storage
      msgIds = getMessageIds();
      assertEquals(20, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 119);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Send 10 more refs
      
      for (int i = 120; i < 130; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);         
         refs[i].releaseMemoryReference();
      }  
      
      //verify 30 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(30, refIds.size());
      assertSameIds(refIds, refs, 100, 129);
      
      //Verify 30 msgs in storage
      msgIds = getMessageIds();
      assertEquals(30, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 129);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Send 10 more refs
      
      for (int i = 130; i < 140; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);         
         refs[i].releaseMemoryReference();
      }  
      
      //verify 40 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //Verify 40 msgs in storage
      msgIds = getMessageIds();
      assertEquals(40, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 139);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());  

      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      
      
      //Send one more ref
      
      msgs[140] = CoreMessageFactory.createCoreMessage(140, false, null);
      refs[140] = ms.reference(msgs[140]);
      queue.handle(null, refs[140], null);
      refs[140].releaseMemoryReference();
      
      //verify 40 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //Verify 40 msgs in storage
      msgIds = getMessageIds();
      assertEquals(40, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 139);
      
      //Verify 100 msgs in store
      assertEquals(101, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());  
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
                
      
      
      //Consume 1
      int consumeCount = 0;
      consume(queue, consumeCount, refs, 1);
      consumeCount++;
      
      //verify 40 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //Verify 40 msgs in storage
      msgIds = getMessageIds();
      assertEquals(40, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 139);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 99 refs in queue
      assertEquals(99, queue.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Now we should have 99 refs in memory, 40 refs in storage, and 1 in down cache, 100 msgs in memory
      
      //Consume 18 more
      consume(queue, consumeCount, refs, 18);
      consumeCount += 18;
      
      //We should have 81 refs in memory, 40 refs in storage, and 1 in down cache, 82 msgs in memory
      
      //verify 40 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(40, refIds.size());
      assertSameIds(refIds, refs, 100, 139);
      
      //Verify 40 msgs in storage
      msgIds = getMessageIds();
      assertEquals(40, msgIds.size()); 
      assertSameIds(msgIds, refs, 100, 139);
      
      //Verify 82 msgs in store
      assertEquals(82, ms.size());
      
      //Verify 81 refs in queue
      assertEquals(81, queue.memoryRefCount());
      
      //Verify 1 refs in downcache
      assertEquals(1, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Consume one more
      
      consume(queue, consumeCount, refs, 1);
      consumeCount++;
      
      //This should force a load of 20 and flush the downcache
      
      //verify 21 refs in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      
      assertEquals(21, refIds.size());
      assertSameIds(refIds, refs, 120, 140);
      
      //Verify 21 msgs in storage
      msgIds = getMessageIds();
      assertEquals(21, msgIds.size()); 
      assertSameIds(msgIds, refs, 120, 140);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Consume 20 more
      
      consume(queue, consumeCount, refs, 20);
      consumeCount += 20;
      
      //verify 1 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(1, refIds.size());
      assertSameIds(refIds, refs, 140, 140);
      
      //Verify 1 msgs in storage
      msgIds = getMessageIds();
      assertEquals(1, msgIds.size()); 
      assertSameIds(msgIds, refs, 140, 140);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      
      //Consume 1 more
      
      consume(queue, consumeCount, refs, 1);
      consumeCount ++;
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
      
      //Verify 81 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 81 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      
      
      //Consume 20 more
      
      consume(queue, consumeCount, refs, 20);
      consumeCount += 20;
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
      
      //Verify 80 msgs in store
      assertEquals(80, ms.size());
      
      //Verify 80 refs in queue
      assertEquals(80, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify not paging
      assertFalse(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      
      //Consumer 60 more
      
            
      consume(queue, consumeCount, refs, 60);
      consumeCount += 60;
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
      
      //Verify 20 msgs in store
      assertEquals(20, ms.size());
      
      //Verify 20 refs in queue
      assertEquals(20, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify not paging
      assertFalse(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
            
      
      
      //Add 20 more messages
      for (int i = 141; i < 161; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);
         refs[i].releaseMemoryReference();
      }
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
      
      //Verify 40 msgs in store
      assertEquals(40, ms.size());
      
      //Verify 40 refs in queue
      assertEquals(40, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify not paging
      assertFalse(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      
      
      //Add 20 more messages
      for (int i = 161; i < 181; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);
         refs[i].releaseMemoryReference();
      }
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
      
      //Verify 60 msgs in store
      assertEquals(60, ms.size());
      
      //Verify 60 refs in queue
      assertEquals(60, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify not paging
      assertFalse(queue.isPaging());
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      
      
      //Add 60 more messages
      for (int i = 181; i < 241; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         refs[i] = ms.reference(msgs[i]);
         queue.handle(null, refs[i], null);
         refs[i].releaseMemoryReference();
      }
      
      //verify 20 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 221, 240);
      
      //Verify 20 msgs in storage
      msgIds = getMessageIds();
      assertEquals(20, msgIds.size()); 
      assertSameIds(msgIds, refs, 221, 240);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify 0 refs in downcache
      assertEquals(0, queue.downCacheCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify no deliveries
      assertEquals(0, queue.getDeliveringCount());;
      
      
       
      //test cancellation
      
      //remove 20 but don't ack them yet
      //this should cause a load to be triggered
      
//      CancellingReceiver r = new CancellingReceiver(20);
//      queue.add(r);
      SimpleDelivery[] dels = getDeliveries(queue, 20);
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
  
      //Verify 120 msgs in store
      assertEquals(120, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify 20 deliveries
      assertEquals(20, queue.getDeliveringCount());;
      
      
       
      //Cancel last 7
      for (int i = 19; i > 12; i--)
      {
         dels[i].cancel();  
      }
      
      // Cancel is asynch, so need to wait a bit
      Thread.sleep(250);       
      
      //This should cause the refs corresponding to the deliveries to go the front of the in memory quuee
      //and the oldest refs in memory evicted off the end into the down cache
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 
  
      //Verify 120 msgs in store
      assertEquals(120, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify 13 deliveries
      assertEquals(13, queue.getDeliveringCount());;
      
      
   
      //Cancel 3 more
      
      for (int i = 12; i > 9; i--)
      {
         dels[i].cancel();
      }
      
      // Cancel is asynch, so need to wait a bit
      Thread.sleep(250);       
      
      //This should cause the down cache to be flushed
      
      //verify 10 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(10, refIds.size());
      assertSameIds(refIds, refs, 231, 240);
      
      //Verify 10 msgs in storage
      msgIds = getMessageIds();
      assertEquals(10, msgIds.size()); 
      assertSameIds(msgIds, refs, 231, 240);
      
      //Verify 110 msgs in store
      assertEquals(110, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify 10 deliveries
      assertEquals(10, queue.getDeliveringCount());;
      
            
      
      //Cancel the last 10
      
      for (int i = 9; i >= 0; i--)
      {
         dels[i].cancel();
      }
      // Cancel is asynch, so need to wait a bit
      Thread.sleep(250);       
      
      //consumeCount += 20;
      
      //This should cause the down cache to be flushed
      
      //verify 20 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(20, refIds.size());
      assertSameIds(refIds, refs, 221, 240);
      
      //Verify 20 msgs in storage
      msgIds = getMessageIds();
      assertEquals(20, msgIds.size()); 
      assertSameIds(msgIds, refs, 221, 240);
      
      //Verify 100 msgs in store
      assertEquals(100, ms.size());
      
      //Verify 100 refs in queue
      assertEquals(100, queue.memoryRefCount());
      
      //Verify paging
      assertTrue(queue.isPaging());      
      
      //Verify 0 deliveries
      assertEquals(0, queue.getDeliveringCount());;
      
      assertEquals(0, queue.downCacheCount());


      //Now there should be 120 message left to consume
      
      //Consume 50
      
      consume(queue, consumeCount, refs, 50);
      consumeCount += 50;
            

      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());     
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 

      //Verify 70 msgs in store
      assertEquals(70, ms.size());
      
      //Verify 0 deliveries
      assertEquals(0, queue.getDeliveringCount());
      
      assertEquals(0, queue.downCacheCount());
                  
      //Verify 70 refs in queue
      assertEquals(70, queue.memoryRefCount());  
      
//    Verify not paging
      assertFalse(queue.isPaging());      
      
      
      
      
      
      
      //Consume the rest
      
      consume(queue, consumeCount, refs, 70);
      consumeCount += 70;
      
      //verify 0 ref in storage
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());     
      
      //Verify 0 msgs in storage
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size()); 

      //Verify 0 msgs in store
      assertEquals(0, ms.size());
      
      //Verify 0 refs in queue
      assertEquals(0, queue.memoryRefCount());
      
      //Make sure there are no more refs in queue
      
      assertEquals(0, queue.getMessageCount());
            
      assertEquals(0, LockMap.instance.getSize());
   }
   

}
