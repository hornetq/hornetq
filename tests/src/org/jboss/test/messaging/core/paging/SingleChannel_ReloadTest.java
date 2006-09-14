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

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.LockMap;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A PagingTest_Reload.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * SingleChannel_Reload.java,v 1.1 2006/03/22 10:23:35 timfox Exp
 */
public class SingleChannel_ReloadTest extends PagingStateTestBase
{
   public SingleChannel_ReloadTest(String name)
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
   
   public void testRecoverableQueueCrash() throws Throwable
   {
      Queue queue = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(), null);
      
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      //Stop and restart the persistence manager
      //Only the persistent messages should survive
      //This is what would happen if the server crashed
       
      pm.stop();
      tr.stop();
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
       
      Queue queue2 = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(), null);
      
      queue2.load();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(50, refIds.size());
                   
      this.consume(queue2, 150, refs, 50);
      
      refIds = getReferenceIdsOrderedByPageOrd(queue2.getChannelID());
      assertEquals(0, refIds.size());
                                                
      assertEquals(0, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(0, queue2.messageCount());
      
      assertEquals(0, LockMap.instance.getSize());
   }
   
   public void testNonRecoverableQueueCrash() throws Throwable
   {
      //Non recoverable queue - eg temporary queue
      
      Queue queue = new Queue(1, ms, pm, true, false, 100, 20, 10, new QueuedExecutor(), null);

      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      //Stop and restart the persistence manager
      //No messages should survive
      //This is what would happen if the server crashed

      pm.stop();
      tr.stop();
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      Queue queue2 = new Queue(1, ms, pm, true, false, 100, 20, 10, new QueuedExecutor(), null);
      
      queue2.load();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      List msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(0, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(0, queue2.messageCount());
      
      assertEquals(0, LockMap.instance.getSize());
   }
   
   public void testNonRecoverableQueueRemoveAllReferences() throws Throwable
   {
      //Non recoverable queue - eg temporary queue
      
      Queue queue = new Queue(1, ms, pm, true, false, 100, 20, 10, new QueuedExecutor(), null);
      
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      queue.removeAllReferences();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      List msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(0, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertFalse(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      assertEquals(0, queue.messageCount());
      
      assertEquals(0, LockMap.instance.getSize());
   }
   
   public void testQueueReloadWithSmallerFullSize() throws Throwable
   {
      Queue queue = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(), null);

      Message[] msgs = new Message[150];
      
      MessageReference[] refs = new MessageReference[150];
       
      //Send 150 p messages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(150, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      //Also we cancel some so we make sure that we have some negative page orders
      
      //cancel 10
      
      this.cancelDeliveries(queue, 10);
      
      //Stop and restart the persistence manager

      pm.stop();
      tr.stop();
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      //Reload the queue with a smaller fullSize
      
      Queue queue2 = new Queue(1, ms, pm, true, false, 50, 20, 10, new QueuedExecutor(), null);
      
      queue2.load();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(150, refIds.size());
      
      List msgIds = getMessageIds();
      assertEquals(150, msgIds.size());
                                                                  
      assertEquals(50, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertTrue(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(50, queue2.messageCount());
      
      //Consume all the messages
      this.consume(queue2, 0, refs, 150);
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(0, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(0, queue2.messageCount());
      
      assertEquals(0, LockMap.instance.getSize());            
   }
   
   public void testReloadWithLargerFullSize() throws Throwable
   {
      Queue queue = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(), null);

      Message[] msgs = new Message[150];
      
      MessageReference[] refs = new MessageReference[150];
       
      //Send 150 p messages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         queue.handle(null, refs[i], null); 
         
         refs[i].releaseMemoryReference();
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(150, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.memoryDeliveryCount());
      
      //Also we cancel some so we make sure that we have some negative page orders
      
      //cancel 10
      
      this.cancelDeliveries(queue, 10);
      
      //Stop and restart the persistence manager

      pm.stop();
      tr.stop();
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      //Reload the queue with a smaller fullSize
      
      Queue queue2 = new Queue(1, ms, pm, true, false, 130, 20, 10, new QueuedExecutor(), null);
      
      queue2.load();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(150, refIds.size());
      
      List msgIds = getMessageIds();
      assertEquals(150, msgIds.size());
                                                                  
      assertEquals(130, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertTrue(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(130, queue2.messageCount());
      
      //Consume all the messages
      this.consume(queue2, 0, refs, 150);
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(0, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.memoryDeliveryCount());
      
      assertEquals(0, queue2.messageCount());
      
      assertEquals(0, LockMap.instance.getSize());            
   }
   
}
