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
import org.jboss.messaging.core.tx.Transaction;


/**
 * 
 * A ChannelShare_P_T_Recoverable.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * ChannelShare_P_T.java,v 1.1 2006/03/22 10:23:35 timfox Exp
 */
public class ChannelShare_P_T extends PagingStateTestBase
{
   public ChannelShare_P_T(String name)
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
      Channel queue1 = new Queue(1, ms, pm, true, 100, 20, 10);
      
      ChannelState state1 = new ChannelState(queue1, pm, true, true, 100, 20, 10);
      
      Channel queue2 = new Queue(2, ms, pm, true, 50, 10, 5);
      
      ChannelState state2 = new ChannelState(queue2, pm, true, true, 50, 10, 5);
                  
      Message[] msgs = new Message[150];
      
      MessageReference[] refs1 = new MessageReference[150];
      
      MessageReference[] refs2 = new MessageReference[150];
      
      //Send 50 refs to both channels
      Transaction tx = tr.createTransaction();
      for (int i = 0; i < 50; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         
         refs1[i] = ms.reference(msgs[i]);
                
         state1.addReference(refs1[i], tx); 
         
         refs2[i] = ms.reference(msgs[i]);
         
         state2.addReference(refs2[i], tx); 
      }
      tx.commit();
      
      //Queue1
      List refIds = getUnloadedReferenceIds(queue1.getChannelID());
      assertEquals(0, refIds.size());
      
      refIds = getReferenceIds(queue1.getChannelID());
      assertEquals(50, refIds.size());
                                    
      assertEquals(50, state1.memoryRefCount());
      
      assertEquals(0, state1.downCacheCount());
      
      assertFalse(state1.isPaging());      
      
      assertEquals(0, state1.memoryDeliveryCount());
      
      //Queue2
      
      refIds = getUnloadedReferenceIds(queue2.getChannelID());
      assertEquals(0, refIds.size());
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(50, refIds.size());
                              
      assertEquals(50, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertTrue(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
            
      //Msgs
      
      assertEquals(50, ms.size());
      
      List msgIds = getMessageIds();
      assertEquals(50, msgIds.size()); 
      
      //Add 25 more
      tx = tr.createTransaction();
      for (int i = 50; i < 75; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         
         refs1[i] = ms.reference(msgs[i]);
                
         state1.addReference(refs1[i], tx); 
         
         refs2[i] = ms.reference(msgs[i]);
         
         state2.addReference(refs2[i], tx); 
      }
      tx.commit();
      
      //Queue1
      refIds = getUnloadedReferenceIds(queue1.getChannelID());
              
      assertEquals(0, refIds.size());
      
      refIds = getReferenceIds(queue1.getChannelID());
      assertEquals(75, refIds.size());
                                    
      assertEquals(75, state1.memoryRefCount());
      
      assertEquals(0, state1.downCacheCount());
      
      assertFalse(state1.isPaging());      
      
      assertEquals(0, state1.memoryDeliveryCount());
      
      //Queue2
      
      refIds = getUnloadedReferenceIds(queue2.getChannelID());
      assertEquals(25, refIds.size());
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(75, refIds.size());
                              
      assertEquals(50, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertTrue(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
            
      //Msgs
      
      assertEquals(75, ms.size());
      
      msgIds = getMessageIds();
      assertEquals(75, msgIds.size());
      
      
      
      // Add 25 more
      tx = tr.createTransaction();
      for (int i = 75; i < 100; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         
         refs1[i] = ms.reference(msgs[i]);
                
         state1.addReference(refs1[i], tx); 
         
         refs2[i] = ms.reference(msgs[i]);
         
         state2.addReference(refs2[i], tx); 
      }
      tx.commit();
      
      //Queue1
      refIds = getUnloadedReferenceIds(queue1.getChannelID());
                
      assertEquals(0, refIds.size());
      
      refIds = getReferenceIds(queue1.getChannelID());
      assertEquals(100, refIds.size());
                                    
      assertEquals(100, state1.memoryRefCount());
      
      assertEquals(0, state1.downCacheCount());
      
      assertTrue(state1.isPaging());      
      
      assertEquals(0, state1.memoryDeliveryCount());
      
      //Queue2
      
      refIds = getUnloadedReferenceIds(queue2.getChannelID());
      assertEquals(50, refIds.size());
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(100, refIds.size());
                              
      assertEquals(50, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertTrue(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
            
      //Msgs
      
      assertEquals(100, ms.size());
      
      msgIds = getMessageIds();
      assertEquals(100, msgIds.size());
      
      
      // Add 50 more
      tx = tr.createTransaction();
      for (int i = 100; i < 150; i++)
      {
         msgs[i] = MessageFactory.createCoreMessage(i, true, null);
         
         refs1[i] = ms.reference(msgs[i]);
                
         state1.addReference(refs1[i], tx); 
         
         refs2[i] = ms.reference(msgs[i]);
         
         state2.addReference(refs2[i], tx); 
      }
      tx.commit();
      
      //Queue1
      refIds = getUnloadedReferenceIds(queue1.getChannelID());
                
      assertEquals(50, refIds.size());
      
      refIds = getReferenceIds(queue1.getChannelID());
      assertEquals(150, refIds.size());
                                    
      assertEquals(100, state1.memoryRefCount());
      
      assertEquals(0, state1.downCacheCount());
      
      assertTrue(state1.isPaging());      
      
      assertEquals(0, state1.memoryDeliveryCount());
      
      //Queue2
      
      refIds = getUnloadedReferenceIds(queue2.getChannelID());
      assertEquals(100, refIds.size());
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(150, refIds.size());
                              
      assertEquals(50, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertTrue(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
            
      //Msgs
      
      assertEquals(100, ms.size());
      
      msgIds = getMessageIds();
      assertEquals(150, msgIds.size());
      
      //    Remove 100 then cancel
      SimpleDelivery[] dels1 = new SimpleDelivery[100];
      for (int i = 0; i < 100; i++)
      {
         MessageReference ref = state1.removeFirstInMemory();
         assertNotNull(ref);       
         dels1[i] = new SimpleDelivery(queue1, ref, false);
         state1.addDelivery(dels1[i]);
      }
      
      SimpleDelivery[] dels2 = new SimpleDelivery[100];
      for (int i = 0; i < 100; i++)
      {
         MessageReference ref = state2.removeFirstInMemory();
         assertNotNull(ref);       
         dels2[i] = new SimpleDelivery(queue2, ref, false);
         state2.addDelivery(dels2[i]);
      }
      
      for (int i = 99; i >=0; i--)
      {
         state1.cancelDelivery(dels1[i]);
      }
      for (int i = 99; i >=0; i--)
      {
         state2.cancelDelivery(dels2[i]);
      }   
      
      //Now consume them all
      
      this.consumeInTx(queue1, state1, 0, refs1, 150);
       
      this.consumeInTx(queue2, state2, 0, refs2, 150);
      
      //    Queue1
      refIds = getReferenceIds(queue1.getChannelID());
                
      assertEquals(0, refIds.size());
                                    
      assertEquals(0, state1.memoryRefCount());
      
      assertEquals(0, state1.downCacheCount());
      
      assertFalse(state1.isPaging());      
      
      assertEquals(0, state1.memoryDeliveryCount());
      
      //Queue2
      
      refIds = getReferenceIds(queue2.getChannelID());
      assertEquals(0, refIds.size());
                              
      assertEquals(0, state2.memoryRefCount());
      
      assertEquals(0, state2.downCacheCount());
      
      assertFalse(state2.isPaging());      
      
      assertEquals(0, state2.memoryDeliveryCount());
            
      //Msgs
      
      assertEquals(0, ms.size());
      
      msgIds = getMessageIds();
      assertEquals(0, msgIds.size());
      
      //Should be none left
      
      assertNull(state1.removeFirstInMemory());
      
      assertNull(state2.removeFirstInMemory());
      
   }

}


