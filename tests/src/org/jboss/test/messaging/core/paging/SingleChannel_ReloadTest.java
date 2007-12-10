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

import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.test.messaging.util.CoreMessageFactory;

import java.util.List;

/**
 * 
 * A PagingTest_Reload.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
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
      MessagingQueue queue = new MessagingQueue(1, "queue1", 1, ms, getPersistenceManager(), true, -1, null, 100, 20, 10, false, 300000);
      queue.activate();
      
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.getDeliveringCount());
      
      //Stop and restart the persistence manager
      //All the paged refs will survive

      tr.stop();
      ms.stop();
      
     /* pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100, !sc.getDatabaseName().equals("oracle"));   
      ((JDBCPersistenceManager)pm).injectNodeID(1);*/
      
      ms = new SimpleMessageStore();
      ms.start();
      
      tr = new TransactionRepository(getPersistenceManager(), ms, idm);

      tr.start();
         
      MessagingQueue queue2 = new MessagingQueue(1, "queue1", 1, ms, getPersistenceManager(), true, -1, null, 100, 20, 10, false, 300000);
    
      queue2.load();
      queue2.activate();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue2.getChannelID());
      assertEquals(50, refIds.size());
      
      assertEquals(100, queue2.memoryRefCount());
      assertEquals(0, queue2.downCacheCount());
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.getDeliveringCount());
                   
      this.consume(queue2, 100, refs, 100);
      
      refIds = getReferenceIdsOrderedByPageOrd(queue2.getChannelID());
      assertEquals(0, refIds.size());
                                                
      assertEquals(0, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      assertEquals(0, queue2.getDeliveringCount());
      
      assertEquals(0, queue2.getMessageCount());
   }
   
   public void testNonRecoverableQueueCrash() throws Throwable
   {
      //Non recoverable queue - eg temporary queue
      
      MessagingQueue queue = new MessagingQueue(1, "queue1", 1, ms, getPersistenceManager(), false, -1, null, 100, 20, 10, false, 300000);
      queue.activate();
         	      
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.getDeliveringCount());
      
      //Stop and restart      

      getPersistenceManager().stop();
      tr.stop();
      ms.stop();
      
      /*getPersistenceManager() =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100, !sc.getDatabaseName().equals("oracle")); 
      ((JDBCPersistenceManager)getPersistenceManager()).injectNodeID(1);*/
      getPersistenceManager().start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      tr = new TransactionRepository(getPersistenceManager(), ms, idm);
      tr.start();

      MessagingQueue queue2 = new MessagingQueue(1, "queue1", 1, ms, getPersistenceManager(), false, -1, null, 100, 20, 10, false, 300000);
      
      queue2.load();
      
      queue2.activate();
      
      //There should be none in the db - the queue is non recoverable
      refIds = getReferenceIdsOrderedByPageOrd(queue2.getChannelID());
      assertEquals(0, refIds.size());
      
      ;
      List msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(100, queue2.memoryRefCount());
      
      assertEquals(0, queue2.downCacheCount());
      
      assertFalse(queue2.isPaging());      
      
      this.consume(queue2, 100, refs, 100);
      
      assertEquals(0, queue2.getDeliveringCount());
      
      assertEquals(0, queue2.getMessageCount());
   }
   
   public void testNonRecoverableQueueRemoveAllReferences() throws Throwable
   {
      //Non recoverable queue - eg temporary queue
      
      MessagingQueue queue = new MessagingQueue(1, "queue1", 1, ms, getPersistenceManager(), false, -1, null, 100, 20, 10, false, 300000);
      queue.activate();
        
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }

      List refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertTrue(queue.isPaging());      
      
      assertEquals(0, queue.getDeliveringCount());
      
      queue.removeAllReferences();
      
      refIds = getReferenceIdsOrderedByPageOrd(queue.getChannelID());
      assertEquals(0, refIds.size());
      
      ;
      List msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
                                                                  
      assertEquals(0, queue.memoryRefCount());
      
      assertEquals(0, queue.downCacheCount());
      
      assertFalse(queue.isPaging());      
      
      assertEquals(0, queue.getDeliveringCount());
      
      assertEquals(0, queue.getMessageCount());
   }
   
   //http://jira.jboss.org/jira/browse/JBMESSAGING-1139
   //If the downcache is not full when we stop the server, we need to test that when we start it again
   //it loads ok (previously it wasn't)
   
   //First test with downcach never flushed
   /*public void testRecoverableQueueRestartWithDownCache() throws Throwable
   {
      testRecoverableQueueRestartWithDownCache(110);
   }
   
   //Then with down cache flushed once 
   public void testRecoverableQueueRestartWithDownCacheAlreadyFlushed() throws Throwable
   {
      testRecoverableQueueRestartWithDownCache(130);
   }*/
   
  /* private void testRecoverableQueueRestartWithDownCache(int num) throws Throwable
   {
      MessagingQueue queue =
         new MessagingQueue(1, "queue1", 1, ms, pm, true, -1, null, 100, 20, 20, false, 300000);
      queue.activate();
      
      Message[] msgs = new Message[num];
      
      MessageReference[] refs = new MessageReference[num];
            
      for (int i = 0; i < num; i++)
      {
         msgs[i] = CoreMessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = msgs[i].createReference();
                
         queue.handle(null, refs[i], null); 
      }
      
            
      pm.stop();
      tr.stop();
      ms.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100, !sc.getDatabaseName().equals("oracle"));   
      ((JDBCPersistenceManager)pm).injectNodeID(1);
      pm.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      tr = new TransactionRepository(pm, ms, idm);

      tr.start();
         
      MessagingQueue queue2 =
         new MessagingQueue(1, "queue1", 1, ms, pm, true, -1, null, 100, 20, 20, false, 300000);
    
      queue2.load();
      queue2.activate();
                              
      this.consume(queue2, 0, refs, num);            
   }*/
     
  
}
