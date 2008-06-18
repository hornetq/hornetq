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

package org.jboss.messaging.tests.unit.core.server.impl.timing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.easymock.EasyMock;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.QueueImpl;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A timing-sensitive QueueTest
 * 
 * All the queue tests which are timing sensitive - go in here 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class QueueTest extends UnitTestCase
{
   private static final long TIMEOUT = 10000;
   
   private static final Logger log = Logger.getLogger(QueueTest.class);
   
   private ScheduledExecutorService scheduledExecutor;
   
   public void setUp() throws Exception
   {
   	super.setUp();
   	
   	scheduledExecutor = new ScheduledThreadPoolExecutor(1);
   }
   
   public void tearDown() throws Exception
   {
   	super.tearDown();
   	
   	scheduledExecutor.shutdownNow();
   }
   
   // The tests ----------------------------------------------------------------

   public void testScheduledDirect()
   {
      testScheduled(true); 
   }
   
   public void testScheduledQueueing()
   {
      testScheduled(false); 
   }
   
   public void testScheduledNoConsumer() throws Exception
   {
      Queue queue = new QueueImpl(1, new SimpleString("queue1"), null, false, true, false, -1, scheduledExecutor);
           
      //Send one scheduled
      
      long now = System.currentTimeMillis();
      
      MessageReference ref1 = generateReference(queue, 1);         
      ref1.setScheduledDeliveryTime(now + 7000);      
      queue.addLast(ref1);
      
      //Send some non scheduled messages
      
      MessageReference ref2 = generateReference(queue, 2);              
      queue.addLast(ref2);
      MessageReference ref3 = generateReference(queue, 3);             
      queue.addLast(ref3);
      MessageReference ref4 = generateReference(queue, 4);               
      queue.addLast(ref4);
      
      
      //Now send some more scheduled messages   
           
      MessageReference ref5 = generateReference(queue, 5); 
      ref5.setScheduledDeliveryTime(now + 5000);
      queue.addLast(ref5);
      
      MessageReference ref6 = generateReference(queue, 6); 
      ref6.setScheduledDeliveryTime(now + 4000);
      queue.addLast(ref6);
      
      MessageReference ref7 = generateReference(queue, 7); 
      ref7.setScheduledDeliveryTime(now + 3000);
      queue.addLast(ref7); 
      
      MessageReference ref8 = generateReference(queue, 8); 
      ref8.setScheduledDeliveryTime(now + 6000);
      queue.addLast(ref8);
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      //Scheduled refs are added back to *FRONT* of queue - otherwise if there were many messages in the queue
      //They may get stranded behind a big backlog
      
      refs.add(ref1);
      refs.add(ref8);
      refs.add(ref5);
      refs.add(ref6);
      refs.add(ref7);
      
      refs.add(ref2);
      refs.add(ref3);
      refs.add(ref4);
      
      Thread.sleep(7500);
      
      FakeConsumer consumer = new FakeConsumer();
      
      queue.addConsumer(consumer);
      
      queue.deliver();
      
      assertRefListsIdenticalRefs(refs, consumer.getReferences());               
   }
   
   private void testScheduled(boolean direct)
   {
      Queue queue = new QueueImpl(1, new SimpleString("queue1"), null, false, true, false, -1, scheduledExecutor);
      
      FakeConsumer consumer = null;
      
      if (direct)
      {
         consumer = new FakeConsumer();
         
         queue.addConsumer(consumer);
      }
      
      //Send one scheduled
      
      long now = System.currentTimeMillis();
      
      MessageReference ref1 = generateReference(queue, 1);         
      ref1.setScheduledDeliveryTime(now + 7000);      
      queue.addLast(ref1);
      
      //Send some non scheduled messages
      
      MessageReference ref2 = generateReference(queue, 2);              
      queue.addLast(ref2);
      MessageReference ref3 = generateReference(queue, 3);             
      queue.addLast(ref3);
      MessageReference ref4 = generateReference(queue, 4);               
      queue.addLast(ref4);
      
      
      //Now send some more scheduled messages   
           
      MessageReference ref5 = generateReference(queue, 5); 
      ref5.setScheduledDeliveryTime(now + 5000);
      queue.addLast(ref5);
      
      MessageReference ref6 = generateReference(queue, 6); 
      ref6.setScheduledDeliveryTime(now + 4000);
      queue.addLast(ref6);
      
      MessageReference ref7 = generateReference(queue, 7); 
      ref7.setScheduledDeliveryTime(now + 3000);
      queue.addLast(ref7); 
      
      MessageReference ref8 = generateReference(queue, 8); 
      ref8.setScheduledDeliveryTime(now + 6000);
      queue.addLast(ref8);
      
      if (!direct)
      {
         consumer = new FakeConsumer();
         
         queue.addConsumer(consumer);
         
         queue.deliver();
      }

      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      refs.add(ref2);
      refs.add(ref3);
      refs.add(ref4);
      
      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      
      refs.clear();
      consumer.getReferences().clear();
                  
      MessageReference ref = consumer.waitForNextReference(TIMEOUT);
      assertEquals(ref7, ref);
      long now2 = System.currentTimeMillis();
      assertTrue(now2 - now >= 3000);
      
      ref = consumer.waitForNextReference(TIMEOUT);
      assertEquals(ref6, ref);
      now2 = System.currentTimeMillis();
      assertTrue(now2 - now >= 4000);
      
      ref = consumer.waitForNextReference(TIMEOUT);
      assertEquals(ref5, ref);
      now2 = System.currentTimeMillis();
      assertTrue(now2 - now >= 5000);
      
      ref = consumer.waitForNextReference(TIMEOUT);
      assertEquals(ref8, ref);
      now2 = System.currentTimeMillis();
      assertTrue(now2 - now >= 6000);
      
      ref = consumer.waitForNextReference(TIMEOUT);
      assertEquals(ref1, ref);
      now2 = System.currentTimeMillis();
      assertTrue(now2 - now >= 7000);
      
      assertTrue(consumer.getReferences().isEmpty());            
   }
   
   public void testDeleteAllReferences() throws Exception
   {
      Queue queue = new QueueImpl(1, new SimpleString("queue1"), null, false, true, false, -1, scheduledExecutor);
      
      StorageManager storageManager = EasyMock.createStrictMock(StorageManager.class);
      
      final int numMessages = 10;
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);
         
         ref.getMessage().setDurable(i % 2 == 0);
         
         refs.add(ref);
         
         queue.addLast(ref);
      }
      
      //Add some scheduled too
      
      final long waitTime = 2000;
      
      final int numScheduled = 10;
      
      for (int i = numMessages; i < numMessages + numScheduled; i++)
      {
         MessageReference ref = generateReference(queue, i);
         
         ref.setScheduledDeliveryTime(System.currentTimeMillis() + waitTime);
         
         ref.getMessage().setDurable(i % 2 == 0);
         
         refs.add(ref);
         
         queue.addLast(ref);
      }
      
      
      assertEquals(numMessages + numScheduled, queue.getMessageCount());   
      assertEquals(numScheduled, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      
      //What I expect to get
      
      EasyMock.expect(storageManager.generateTransactionID()).andReturn(1L);

      for (int i = 0; i < numMessages; i++)
      {
      	if (i % 2 == 0)
      	{
      		storageManager.storeDeleteTransactional(1, i);
      	}
      }
      
      for (int i = numMessages; i < numMessages + numScheduled; i++)
      {
      	if (i % 2 == 0)
      	{
      		storageManager.storeDeleteTransactional(1, i);
      	}
      }
      
      storageManager.commit(1);
      
      EasyMock.replay(storageManager);
      
      queue.deleteAllReferences(storageManager);
      
      EasyMock.verify(storageManager);
      
      assertEquals(0, queue.getMessageCount());   
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      
      Thread.sleep(waitTime + 500);
      
      //Make sure scheduled don't arrive
      
      FakeConsumer consumer = new FakeConsumer();
      
      queue.addConsumer(consumer);
      
      queue.deliver();
      
      assertTrue(consumer.getReferences().isEmpty());      
   }
   
   // Inner classes ---------------------------------------------------------------
      
}

