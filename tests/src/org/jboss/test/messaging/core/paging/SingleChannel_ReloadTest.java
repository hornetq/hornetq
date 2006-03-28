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
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.LockMap;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.tx.TransactionRepository;

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
   
   public void testReload() throws Throwable
   {
      Channel queue = new Queue(1, ms, pm, true, 100, 20, 10);
      
      ChannelState state = new ChannelState(queue, pm, true, true, 100, 20, 10);
        
      Message[] msgs = new Message[200];
      
      MessageReference[] refs = new MessageReference[200];
       
      //Send 150 np mesages
      for (int i = 0; i < 150; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, false, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         state.addReference(refs[i]); 
      }
      
      //Send 50 p messages
      for (int i = 150; i < 200; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         
         refs[i] = ms.reference(msgs[i]);
                
         state.addReference(refs[i]); 
      }

      List refIds = getReferenceIds(queue.getChannelID());
      assertEquals(100, refIds.size());
                                                
      assertEquals(100, state.memoryRefCount());
      
      assertEquals(0, state.downCacheCount());
      
      assertTrue(state.isPaging());      
      
      assertEquals(0, state.memoryDeliveryCount());
      
      //Stop and restart the persistence manager
      //Only the persistent messages should survive
       
      tr.stop();
      ms.stop();
      pm.stop();
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());

      ((JDBCPersistenceManager)pm).start();

      ms = new SimpleMessageStore("store1");
      
      tr = new TransactionRepository();
      
      tr.start(pm);
         
      Channel queue2 = new Queue(1, ms, pm, true, 100, 20, 10);
      
      ChannelState state2 = new ChannelState(queue2, pm, true, true, 100, 20, 10);
      
      state2.load();
      
      refIds = getReferenceIds(queue.getChannelID());
      assertEquals(50, refIds.size());
                  
      this.consume(queue2, state2, 150, refs, 50);
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(0, refIds.size());
                                                
      assertEquals(0, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertFalse(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
      
      MessageReference ref = state2.removeFirstInMemory();
      
      assertNull(ref);
      
      assertEquals(0, LockMap.instance.getSize());
   }
}
