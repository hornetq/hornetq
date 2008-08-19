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

package org.jboss.messaging.tests.unit.core.server.impl;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.QueueImpl;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeFilter;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * A QueueTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class QueueImplTest extends UnitTestCase
{
   // The tests ----------------------------------------------------------------

   private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

   private static final SimpleString queue1 = new SimpleString("queue1");

   public void testID()
   {
      final long id = 123;

      Queue queue = new QueueImpl(id, queue1, null, false, true, -1, scheduledExecutor);

      assertEquals(id, queue.getPersistenceID());

      final long id2 = 456;

      queue.setPersistenceID(id2);

      assertEquals(id2, queue.getPersistenceID());
   }

   public void testName()
   {
      final SimpleString name = new SimpleString("oobblle");

      Queue queue = new QueueImpl(1, name, null, false, true, -1, scheduledExecutor);

      assertEquals(name, queue.getName());
   }

   public void testClustered()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertFalse(queue.isClustered());

      queue = new QueueImpl(1, queue1, null, true, true, -1, scheduledExecutor);

      assertTrue(queue.isClustered());
   }

   public void testDurable()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, false, -1, scheduledExecutor);

      assertFalse(queue.isDurable());

      queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertTrue(queue.isDurable());
   }

   public void testGetMaxSizeBytes()
   {
      final int maxSize = 123456;

      final int id = 123;

      Queue queue = new QueueImpl(id, queue1, null, false, true, maxSize, scheduledExecutor);

      assertEquals(id, queue.getPersistenceID());

      assertEquals(maxSize, queue.getMaxSizeBytes());
   }

   public void testAddRemoveConsumer() throws Exception
   {
      Consumer cons1 = new FakeConsumer();

      Consumer cons2 = new FakeConsumer();

      Consumer cons3 = new FakeConsumer();

      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertEquals(0, queue.getConsumerCount());

      queue.addConsumer(cons1);

      assertEquals(1, queue.getConsumerCount());

      assertTrue(queue.removeConsumer(cons1));

      assertEquals(0, queue.getConsumerCount());

      queue.addConsumer(cons1);

      queue.addConsumer(cons2);

      queue.addConsumer(cons3);

      assertEquals(3, queue.getConsumerCount());

      assertFalse(queue.removeConsumer(new FakeConsumer()));

      assertEquals(3, queue.getConsumerCount());

      assertTrue(queue.removeConsumer(cons1));

      assertEquals(2, queue.getConsumerCount());

      assertTrue(queue.removeConsumer(cons2));

      assertEquals(1, queue.getConsumerCount());

      assertTrue(queue.removeConsumer(cons3));

      assertEquals(0, queue.getConsumerCount());

      assertFalse(queue.removeConsumer(cons3));
   }

   public void testGetSetDistributionPolicy()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertNotNull(queue.getDistributionPolicy());

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributionPolicy);

      DistributionPolicy policy = new DummyDistributionPolicy();

      queue.setDistributionPolicy(policy);

      assertEquals(policy, queue.getDistributionPolicy());
   }

   public void testGetFilter()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertNull(queue.getFilter());

      Filter filter = createMock(Filter.class);
      replay(filter);

      queue = new QueueImpl(1, queue1, filter, false, true, -1, scheduledExecutor);

      assertEquals(filter, queue.getFilter());
      
      verify(filter);
   }

   public void testDefaultMaxSize()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertEquals(-1, queue.getMaxSizeBytes());
   }

   public void testSimpleAddLast()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         queue.addLast(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

   }

   public void testSimpleDirectDelivery()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
   }

   public void testSimpleNonDirectDelivery()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      //Now add a consumer
      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      assertTrue(consumer.getReferences().isEmpty());
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());

      queue.deliver();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());
   }

   public void testBusyConsumer()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      FakeConsumer consumer = new FakeConsumer();

      consumer.setStatusImmediate(HandleStatus.BUSY);

      queue.addConsumer(consumer);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      queue.deliver();

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      consumer.setStatusImmediate(HandleStatus.HANDLED);

      queue.deliver();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(10, queue.getDeliveringCount());
   }

   public void testBusyConsumerThenAddMoreMessages()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      FakeConsumer consumer = new FakeConsumer();

      consumer.setStatusImmediate(HandleStatus.BUSY);

      queue.addConsumer(consumer);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      queue.deliver();

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(20, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      consumer.setStatusImmediate(HandleStatus.HANDLED);

      for (int i = numMessages * 2; i < numMessages * 3; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      queue.deliver();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(30, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(30, queue.getDeliveringCount());
   }

   public void testAddFirstAddLast()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 10;

      List<MessageReference> refs1 = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs1.add(ref);

         queue.addLast(ref);
      }

      LinkedList<MessageReference> refs2 = new LinkedList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i + numMessages);

         refs2.addFirst(ref);

         queue.addFirst(ref);
      }

      List<MessageReference> refs3 = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i + 2 * numMessages);

         refs3.add(ref);

         queue.addLast(ref);
      }

      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliver();

      List<MessageReference> allRefs = new ArrayList<MessageReference>();

      allRefs.addAll(refs2);
      allRefs.addAll(refs1);
      allRefs.addAll(refs3);

      assertRefListsIdenticalRefs(allRefs, consumer.getReferences());
   }


   public void testChangeConsumersAndDeliver() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      FakeConsumer cons1 = new FakeConsumer();

      queue.addConsumer(cons1);

      queue.deliver();

      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, cons1.getReferences());

      FakeConsumer cons2 = new FakeConsumer();

      queue.addConsumer(cons2);

      assertEquals(2, queue.getConsumerCount());

      cons1.getReferences().clear();

      for (MessageReference ref : refs)
      {
         queue.referenceAcknowledged(ref);
      }

      refs.clear();

      for (int i = 0; i < 2 * numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages * 2, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages * 2, queue.getDeliveringCount());

      assertEquals(numMessages, cons1.getReferences().size());

      assertEquals(numMessages, cons2.getReferences().size());

      cons1.getReferences().clear();
      cons2.getReferences().clear();

      for (MessageReference ref : refs)
      {
         queue.referenceAcknowledged(ref);
      }
      refs.clear();

      FakeConsumer cons3 = new FakeConsumer();

      queue.addConsumer(cons3);

      assertEquals(3, queue.getConsumerCount());

      for (int i = 0; i < 3 * numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages * 3, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages * 3, queue.getDeliveringCount());

      assertEquals(numMessages, cons1.getReferences().size());

      assertEquals(numMessages, cons2.getReferences().size());

      assertEquals(numMessages, cons3.getReferences().size());

      queue.removeConsumer(cons1);

      cons3.getReferences().clear();
      cons2.getReferences().clear();

      for (MessageReference ref : refs)
      {
         queue.referenceAcknowledged(ref);
      }
      refs.clear();

      for (int i = 0; i < 2 * numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages * 2, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages * 2, queue.getDeliveringCount());

      assertEquals(numMessages, cons2.getReferences().size());

      assertEquals(numMessages, cons3.getReferences().size());

      queue.removeConsumer(cons3);

      cons2.getReferences().clear();

      for (MessageReference ref : refs)
      {
         queue.referenceAcknowledged(ref);
      }
      refs.clear();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());

      assertEquals(numMessages, cons2.getReferences().size());

   }

   public void testConsumerReturningNull()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      class NullConsumer implements Consumer
      {
         public HandleStatus handle(MessageReference reference)
         {
            return null;
         }
      }

      queue.addConsumer(new NullConsumer());

      MessageReference ref = generateReference(queue, 1);

      try
      {
         queue.addLast(ref);

         fail("Should throw IllegalStateException");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
   }

   public void testRoundRobinWithQueueing()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributionPolicy);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      //Test first with queueing

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      FakeConsumer cons1 = new FakeConsumer();

      FakeConsumer cons2 = new FakeConsumer();

      queue.addConsumer(cons1);

      queue.addConsumer(cons2);

      queue.deliver();

      assertEquals(numMessages / 2, cons1.getReferences().size());

      assertEquals(numMessages / 2, cons2.getReferences().size());

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref;

         ref = (i % 2 == 0) ? cons1.getReferences().get(i / 2) : cons2.getReferences().get(i / 2);

         assertEquals(refs.get(i), ref);
      }
   }

   public void testRoundRobinDirect()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributionPolicy);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      FakeConsumer cons1 = new FakeConsumer();

      FakeConsumer cons2 = new FakeConsumer();

      queue.addConsumer(cons1);

      queue.addConsumer(cons2);

      queue.deliver();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(numMessages / 2, cons1.getReferences().size());

      assertEquals(numMessages / 2, cons2.getReferences().size());

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref;

         ref = (i % 2 == 0) ? cons1.getReferences().get(i / 2) : cons2.getReferences().get(i / 2);

         assertEquals(refs.get(i), ref);
      }
   }

   public void testDeleteAllReferences() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

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

      final int numScheduled = 10;

      for (int i = numMessages; i < numMessages + numScheduled; i++)
      {
         MessageReference ref = generateReference(queue, i);

         ref.setScheduledDeliveryTime(System.currentTimeMillis() + 1000000000);

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

      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliver();

      assertTrue(consumer.getReferences().isEmpty());
   }

   public void testMaxSize()
   {
      final int maxSize = 10 * 1024;

      Queue queue = new QueueImpl(1, queue1, null, false, true, maxSize, scheduledExecutor);

      List<MessageReference> refs = new ArrayList<MessageReference>();

      int size = 0;

      int i = 0;
      while (true)
      {
         MessageReference ref = generateReference(queue, i++);

         if (size + ref.getMessage().getEncodeSize() > maxSize)
         {
            break;
         }

         size += ref.getMessage().getEncodeSize();

         refs.add(ref);

         assertEquals(HandleStatus.HANDLED, queue.addLast(ref));
      }

      assertEquals(maxSize, queue.getMaxSizeBytes());
      assertEquals(size, queue.getSizeBytes());

      //Try to add more

      for (int j = 0; j < 10; j++)
      {
         MessageReference ref = generateReference(queue, j);

         assertEquals(HandleStatus.BUSY, queue.addLast(ref));
      }

      assertEquals(maxSize, queue.getMaxSizeBytes());
      assertEquals(size, queue.getSizeBytes());

      // Try to add at front too

      for (int j = 0; j < 10; j++)
      {
         MessageReference ref = generateReference(queue, i);

         assertEquals(HandleStatus.BUSY, queue.addLast(ref));
      }

      assertEquals(maxSize, queue.getMaxSizeBytes());
      assertEquals(size, queue.getSizeBytes());
   }

   public void testWithPriorities()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         ref.getMessage().setPriority((byte) i);

         refs.add(ref);

         assertEquals(HandleStatus.HANDLED, queue.addLast(ref));
      }

      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliver();

      List<MessageReference> receivedRefs = consumer.getReferences();

      //Should be in reverse order

      assertEquals(refs.size(), receivedRefs.size());

      for (int i = 0; i < numMessages; i++)
      {
         assertEquals(refs.get(i), receivedRefs.get(9 - i));
      }

      //But if we send more - since we are now in direct mode - the order will be the send order
      //since the refs don't get queued

      consumer.clearReferences();

      refs.clear();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         ref.getMessage().setPriority((byte) i);

         refs.add(ref);

         assertEquals(HandleStatus.HANDLED, queue.addLast(ref));
      }

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
   }

   public void testConsumerWithFiltersDirect() throws Exception
   {
      testConsumerWithFilters(true);
   }

   public void testConsumerWithFiltersQueueing() throws Exception
   {
      testConsumerWithFilters(false);
   }

   public void testConsumerWithFilterAddAndRemove()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);
   }

   public void testList()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 20;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         queue.addLast(ref);

         refs.add(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());

      List<MessageReference> list = queue.list(null);

      assertRefListsIdenticalRefs(refs, list);
   }

   public void testListWithFilter()
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      final int numMessages = 20;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         if (i % 2 == 0)
         {
            ref.getMessage().putStringProperty(new SimpleString("god"), new SimpleString("dog"));
         }

         queue.addLast(ref);

         refs.add(ref);
      }

      assertEquals(numMessages, queue.getMessageCount());

      Filter filter = new FakeFilter("god", "dog");

      List<MessageReference> list = queue.list(filter);

      assertEquals(numMessages / 2, list.size());

      for (int i = 0; i < numMessages; i += 2)
      {
         assertEquals(refs.get(i), list.get(i / 2));
      }
   }

   public void testConsumeWithFiltersAddAndRemoveConsumer() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);

      queue.addConsumer(consumer);

      List<MessageReference> refs = new ArrayList<MessageReference>();

      MessageReference ref1 = generateReference(queue, 1);

      ref1.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref1));

      MessageReference ref2 = generateReference(queue, 2);

      ref2.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref2));

      refs.add(ref2);


      assertEquals(2, queue.getMessageCount());

      assertEquals(1, consumer.getReferences().size());

      assertEquals(1, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());

      queue.referenceAcknowledged(ref2);

      queue.removeConsumer(consumer);

      queue.addConsumer(consumer);

      queue.deliver();


      refs.clear();

      consumer.clearReferences();

      MessageReference ref3 = generateReference(queue, 3);

      ref3.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref3));

      MessageReference ref4 = generateReference(queue, 4);

      ref4.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref4));

      refs.add(ref4);

      assertEquals(3, queue.getMessageCount());

      assertEquals(1, consumer.getReferences().size());

      assertEquals(1, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
   }

   // Private ------------------------------------------------------------------------------

   private void testConsumerWithFilters(boolean direct) throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);

      if (direct)
      {
         queue.addConsumer(consumer);
      }

      List<MessageReference> refs = new ArrayList<MessageReference>();

      MessageReference ref1 = generateReference(queue, 1);

      ref1.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref1));

      MessageReference ref2 = generateReference(queue, 2);

      ref2.getMessage().putStringProperty(new SimpleString("cheese"), new SimpleString("stilton"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref2));

      MessageReference ref3 = generateReference(queue, 3);

      ref3.getMessage().putStringProperty(new SimpleString("cake"), new SimpleString("sponge"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref3));

      MessageReference ref4 = generateReference(queue, 4);

      ref4.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      refs.add(ref4);

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref4));

      MessageReference ref5 = generateReference(queue, 5);

      ref5.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("apple"));

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref5));

      MessageReference ref6 = generateReference(queue, 6);

      ref6.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      refs.add(ref6);

      assertEquals(HandleStatus.HANDLED, queue.addLast(ref6));

      if (!direct)
      {
         queue.addConsumer(consumer);

         queue.deliver();
      }

      assertEquals(6, queue.getMessageCount());

      assertEquals(2, consumer.getReferences().size());

      assertEquals(2, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());

      queue.referenceAcknowledged(ref5);
      queue.referenceAcknowledged(ref6);

      queue.removeConsumer(consumer);

      consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliver();

      assertEquals(4, queue.getMessageCount());

      assertEquals(4, consumer.getReferences().size());

      assertEquals(4, queue.getDeliveringCount());
   }

   public void testMessageOrder() throws Exception
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addFirst(messageReference);
      queue.addLast(messageReference2);
      queue.addFirst(messageReference3);
      EasyMock.expect(consumer.handle(messageReference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer);
      queue.addConsumer(consumer);
      queue.deliver();
      EasyMock.verify(consumer);
   }

   public void testMessagesAdded() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addLast(messageReference);
      queue.addLast(messageReference2);
      queue.addLast(messageReference3);
      assertEquals(queue.getMessagesAdded(), 3);
   }

   public void testAddLastWhenLocked() throws Exception
   {

      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      queue.lock();
      CountDownLatch countDownLatch = new CountDownLatch(1);
      AddtoQueueRunner runner = new AddtoQueueRunner(false, queue, messageReference, countDownLatch);
      new Thread(runner).start();
      assertFalse(runner.added);
      queue.unlock();
      countDownLatch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(runner.added);

   }

   public void testAddLastWhenLockedMultiple() throws Exception
   {

      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.lock();
      CountDownLatch countDownLatch = new CountDownLatch(3);
      AddtoQueueRunner runner = new AddtoQueueRunner(false, queue, messageReference, countDownLatch);
      new Thread(runner).start();
      AddtoQueueRunner runner2 = new AddtoQueueRunner(false, queue, messageReference2, countDownLatch);
      new Thread(runner2).start();
      AddtoQueueRunner runner3 = new AddtoQueueRunner(false, queue, messageReference3, countDownLatch);
      new Thread(runner3).start();
      assertFalse(runner.added);
      assertFalse(runner2.added);
      assertFalse(runner3.added);
      queue.unlock();
      countDownLatch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(runner.added);
      assertTrue(runner2.added);
      assertTrue(runner3.added);

   }

   public void testAddFirstWhenLocked() throws Exception
   {

      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      queue.lock();
      CountDownLatch countDownLatch = new CountDownLatch(1);
      AddtoQueueRunner runner = new AddtoQueueRunner(true, queue, messageReference, countDownLatch);
      new Thread(runner).start();
      assertFalse(runner.added);
      queue.unlock();
      countDownLatch.await(1000, TimeUnit.MILLISECONDS);
      assertTrue(runner.added);

   }

   public void testAddFirstWhenLockedMultiple() throws Exception
   {

      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.lock();
      CountDownLatch countDownLatch = new CountDownLatch(3);
      AddtoQueueRunner runner = new AddtoQueueRunner(true, queue, messageReference, countDownLatch);
      new Thread(runner).start();
      AddtoQueueRunner runner2 = new AddtoQueueRunner(true, queue, messageReference2, countDownLatch);
      new Thread(runner2).start();
      AddtoQueueRunner runner3 = new AddtoQueueRunner(true, queue, messageReference3, countDownLatch);
      new Thread(runner3).start();
      assertFalse(runner.added);
      assertFalse(runner2.added);
      assertFalse(runner3.added);
      queue.unlock();
      countDownLatch.await(10000, TimeUnit.MILLISECONDS);
      assertTrue(runner.added);
      assertTrue(runner2.added);
      assertTrue(runner3.added);

   }

   public void testAddListFirst() throws Exception
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      LinkedList<MessageReference> messageReferences = new LinkedList<MessageReference>();
      messageReferences.add(messageReference);
      messageReferences.add(messageReference2);
      messageReferences.add(messageReference3);
      EasyMock.expect(consumer.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer);
      queue.addConsumer(consumer);
      queue.addListFirst(messageReferences);
      EasyMock.verify(consumer);

   }

   public void testRemoveReferenceWithId() throws Exception
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      LinkedList<MessageReference> messageReferences = new LinkedList<MessageReference>();
      messageReferences.add(messageReference);
      messageReferences.add(messageReference2);
      messageReferences.add(messageReference3);
      EasyMock.expect(consumer.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer);
      queue.addListFirst(messageReferences);
      queue.removeReferenceWithID(2);
      queue.addConsumer(consumer);
      queue.deliver();
      EasyMock.verify(consumer);

   }

   public void testGetReference() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addFirst(messageReference);
      queue.addFirst(messageReference2);
      queue.addFirst(messageReference3);
      assertEquals(queue.getReference(2), messageReference2);

   }

   public void testGetNonExistentReference() throws Exception
   {
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addFirst(messageReference);
      queue.addFirst(messageReference2);
      queue.addFirst(messageReference3);
      assertNull(queue.getReference(5));

   }

   public void testConsumerRemovedAfterException() throws Exception
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      LinkedList<MessageReference> messageReferences = new LinkedList<MessageReference>();
      messageReferences.add(messageReference);
      messageReferences.add(messageReference2);
      messageReferences.add(messageReference3);
      EasyMock.expect(consumer.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference2)).andThrow(new RuntimeException());
      EasyMock.replay(consumer);
      queue.addConsumer(consumer);
      queue.addListFirst(messageReferences);
      EasyMock.verify(consumer);

   }

   public void testDeliveryAsync() throws Exception
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      LinkedList<MessageReference> messageReferences = new LinkedList<MessageReference>();
      messageReferences.add(messageReference);
      messageReferences.add(messageReference2);
      messageReferences.add(messageReference3);
      EasyMock.expect(consumer.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(messageReference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer);
      queue.addListFirst(messageReferences);
      queue.addConsumer(consumer);
      queue.deliverAsync(new Executor()
      {
         public void execute(Runnable command)
         {
            command.run();
         }
      });
      EasyMock.verify(consumer);

   }
   
   public void testExpireMessage() throws Exception
   {
      long messageID = randomLong();
      final SimpleString expiryQueue = new SimpleString("expiryQueue");
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, messageID);
      StorageManager storageManager = EasyMock.createMock(StorageManager.class);
      EasyMock.expect(storageManager.generateTransactionID()).andReturn(randomLong());
      EasyMock.expect(storageManager.generateMessageID()).andReturn(randomLong());
      storageManager.storeDeleteTransactional(EasyMock.anyLong(), EasyMock.eq(messageID));
      storageManager.commit(EasyMock.anyLong());
      PostOffice postOffice = createMock(PostOffice.class);      
      Binding expiryBinding = createMock(Binding.class);
      EasyMock.expect(expiryBinding.getAddress()).andStubReturn(expiryQueue);
      EasyMock.expect(postOffice.getBinding(expiryQueue)).andReturn(expiryBinding );
      EasyMock.expect(postOffice.route(EasyMock.isA(ServerMessage.class))).andReturn(new ArrayList<MessageReference>());
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      QueueSettings queueSettings = new QueueSettings() 
      {
         @Override
         public SimpleString getExpiryQueue()
         {
            return expiryQueue;
         } 
      };
      EasyMock.expect(queueSettingsRepository.getMatch(queue1.toString())).andStubReturn(queueSettings);

      EasyMock.replay(storageManager, postOffice, queueSettingsRepository, expiryBinding);

      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());
      
      queue.addLast(messageReference);
      
      assertEquals(1, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(queue.getSizeBytes() > 0);
      
      queue.expireMessage(messageID, storageManager , postOffice, queueSettingsRepository);
      
      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());

      EasyMock.verify(storageManager, postOffice, queueSettingsRepository, expiryBinding);
   }

   public void testSendMessageToDLQ() throws Exception
   {
      long messageID = randomLong();
      final SimpleString dlqName = new SimpleString("dlq");
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      MessageReference messageReference = generateReference(queue, messageID);
      StorageManager storageManager = createMock(StorageManager.class);
      expect(storageManager.generateTransactionID()).andReturn(randomLong());
      expect(storageManager.generateMessageID()).andReturn(randomLong());
      storageManager.storeDeleteTransactional(anyLong(), eq(messageID));
      storageManager.commit(anyLong());
      PostOffice postOffice = createMock(PostOffice.class);      
      Binding dlqBinding = createMock(Binding.class);
      expect(dlqBinding.getAddress()).andStubReturn(dlqName);
      expect(postOffice.getBinding(dlqName)).andReturn(dlqBinding );
      expect(postOffice.route(isA(ServerMessage.class))).andReturn(new ArrayList<MessageReference>());
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);
      QueueSettings queueSettings = new QueueSettings() 
      {
         @Override
         public SimpleString getDLQ()
         {
            return dlqName;
         } 
      };
      EasyMock.expect(queueSettingsRepository.getMatch(queue1.toString())).andStubReturn(queueSettings);

      EasyMock.replay(storageManager, postOffice, queueSettingsRepository, dlqBinding);

      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());
      
      queue.addLast(messageReference);
      
      assertEquals(1, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(queue.getSizeBytes() > 0);
      
      queue.sendMessageToDLQ(messageID, storageManager , postOffice, queueSettingsRepository);
      
      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());

      EasyMock.verify(storageManager, postOffice, queueSettingsRepository, dlqBinding);
   }
   
   public void testMoveMessage() throws Exception
   {
      long messageID = randomLong();
      long newMessageID = randomLong();
      long tid = randomLong();
      final SimpleString toQueueName = new SimpleString("toQueueName");
      Queue queue = new QueueImpl(1, queue1, null, false, true, -1, scheduledExecutor);
      Queue toQueue = createMock(Queue.class);
    
      MessageReference messageReference = generateReference(queue, messageID);
      StorageManager storageManager = EasyMock.createMock(StorageManager.class);
      EasyMock.expect(storageManager.generateMessageID()).andReturn(newMessageID);
      EasyMock.expect(storageManager.generateTransactionID()).andReturn(tid);
      storageManager.storeDeleteTransactional(EasyMock.anyLong(), EasyMock.eq(messageID));
      storageManager.commit(EasyMock.anyLong());
      PostOffice postOffice = EasyMock.createMock(PostOffice.class);      
      Binding toBinding = EasyMock.createMock(Binding.class);
      EasyMock.expect(toBinding.getAddress()).andStubReturn(toQueueName);
      EasyMock.expect(toBinding.getQueue()).andStubReturn(toQueue);
      EasyMock.expect(postOffice.route(EasyMock.isA(ServerMessage.class))).andReturn(new ArrayList<MessageReference>());
      HierarchicalRepository<QueueSettings> queueSettingsRepository = EasyMock.createMock(HierarchicalRepository.class);

      EasyMock.replay(storageManager, postOffice, queueSettingsRepository, toBinding);

      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());
      
      queue.addLast(messageReference);
      
      assertEquals(1, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(queue.getSizeBytes() > 0);
      
      queue.moveMessage(messageID, toBinding, storageManager, postOffice);
      
      assertEquals(0, queue.getMessageCount());
      assertEquals(0, queue.getDeliveringCount());
      assertEquals(0, queue.getSizeBytes());

      EasyMock.verify(storageManager, postOffice, queueSettingsRepository, toBinding);
   }
   
   // Inner classes ---------------------------------------------------------------

   class AddtoQueueRunner implements Runnable
   {
      Queue queue;
      MessageReference messageReference;
      boolean added = false;
      CountDownLatch countDownLatch;
      boolean first;

      public AddtoQueueRunner(boolean first, Queue queue, MessageReference messageReference, CountDownLatch countDownLatch)
      {
         this.queue = queue;
         this.messageReference = messageReference;
         this.countDownLatch = countDownLatch;
         this.first = first;
      }

      public void run()
      {
         if (first)
         {
            queue.addFirst(messageReference);
         }
         else
         {
            queue.addLast(messageReference);
         }
         added = true;
         countDownLatch.countDown();
      }
   }

   class DummyDistributionPolicy implements DistributionPolicy
   {
      public int select(List<Consumer> consumers, int lastPos)
      {
         return 0;
      }
   }

}
