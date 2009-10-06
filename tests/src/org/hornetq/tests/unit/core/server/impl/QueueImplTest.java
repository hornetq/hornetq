/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.core.server.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.Distributor;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.server.impl.RoundRobinDistributor;
import org.hornetq.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.hornetq.tests.unit.core.server.impl.fakes.FakeFilter;
import org.hornetq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   private static final SimpleString address1 = new SimpleString("address1");

   public void testName()
   {
      final SimpleString name = new SimpleString("oobblle");

      Queue queue = new QueueImpl(1, address1, name, null, false, true, scheduledExecutor, null, null, null);

      assertEquals(name, queue.getName());
   }

   public void testDurable()
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, false, scheduledExecutor, null, null, null);

      assertFalse(queue.isDurable());

      queue = new QueueImpl(1, address1, queue1, null, true, false, scheduledExecutor, null, null, null);

      assertTrue(queue.isDurable());
   }

   public void testAddRemoveConsumer() throws Exception
   {
      Consumer cons1 = new FakeConsumer();

      Consumer cons2 = new FakeConsumer();

      Consumer cons3 = new FakeConsumer();

      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      assertNotNull(queue.getDistributionPolicy());

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributor);

      Distributor policy = new DummyDistributionPolicy();

      queue.setDistributionPolicy(policy);

      assertEquals(policy, queue.getDistributionPolicy());
   }

   public void testGetFilter()
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      assertNull(queue.getFilter());

      Filter filter = new Filter()
      {
         public boolean match(ServerMessage message)
         {
            return false;
         }

         public SimpleString getFilterString()
         {
            return null;
         }
      };

      queue = new QueueImpl(1, address1, queue1, filter, false, true, scheduledExecutor, null, null, null);

      assertEquals(filter, queue.getFilter());

   }

   public void testSimpleadd()
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

   public void testSimpleDirectDelivery() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

   public void testSimpleNonDirectDelivery() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

      // Now add a consumer
      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      assertTrue(consumer.getReferences().isEmpty());
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());

      queue.deliverNow();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());
   }

   public void testBusyConsumer() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

      queue.deliverNow();

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      consumer.setStatusImmediate(HandleStatus.HANDLED);

      queue.deliverNow();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(10, queue.getDeliveringCount());
   }

   public void testBusyConsumerThenAddMoreMessages() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

      queue.deliverNow();

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

      queue.deliverNow();

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(30, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(30, queue.getDeliveringCount());
   }

   public void testAddFirstadd() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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

      queue.deliverNow();

      List<MessageReference> allRefs = new ArrayList<MessageReference>();

      allRefs.addAll(refs2);
      allRefs.addAll(refs1);
      allRefs.addAll(refs3);

      assertRefListsIdenticalRefs(allRefs, consumer.getReferences());
   }

   public void testChangeConsumersAndDeliver() throws Exception
   {
      Queue queue = new QueueImpl(1,
                                  address1,
                                  queue1,
                                  null,
                                  false,
                                  true,
                                  scheduledExecutor,
                                  new FakePostOffice(),
                                  null,
                                  null);

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

      queue.deliverNow();

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
         queue.acknowledge(ref);
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
         queue.acknowledge(ref);
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
         queue.acknowledge(ref);
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
         queue.acknowledge(ref);
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

   public void testConsumerReturningNull() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      class NullConsumer implements Consumer
      {
         public Filter getFilter()
         {
            return null;
         }

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
         // Ok
      }
   }

   public void testRoundRobinWithQueueing() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributor);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      // Test first with queueing

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

      queue.deliverNow();

      assertEquals(numMessages / 2, cons1.getReferences().size());

      assertEquals(numMessages / 2, cons2.getReferences().size());

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref;

         ref = (i % 2 == 0) ? cons1.getReferences().get(i / 2) : cons2.getReferences().get(i / 2);

         assertEquals(refs.get(i), ref);
      }
   }

   public void testRoundRobinDirect() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      assertTrue(queue.getDistributionPolicy() instanceof RoundRobinDistributor);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      FakeConsumer cons1 = new FakeConsumer();

      FakeConsumer cons2 = new FakeConsumer();

      queue.addConsumer(cons1);

      queue.addConsumer(cons2);

      queue.deliverNow();

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

   public void testWithPriorities() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         ref.getMessage().setPriority((byte)i);

         refs.add(ref);

         queue.addLast(ref);
      }

      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliverNow();

      List<MessageReference> receivedRefs = consumer.getReferences();

      // Should be in reverse order

      assertEquals(refs.size(), receivedRefs.size());

      for (int i = 0; i < numMessages; i++)
      {
         assertEquals(refs.get(i), receivedRefs.get(9 - i));
      }

      // But if we send more - since we are now in direct mode - the order will be the send order
      // since the refs don't get queued

      consumer.clearReferences();

      refs.clear();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         ref.getMessage().setPriority((byte)i);

         refs.add(ref);

         queue.addLast(ref);
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
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);
   }

   public void testList()
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

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
      Queue queue = new QueueImpl(1,
                                  address1,
                                  queue1,
                                  null,
                                  false,
                                  true,
                                  scheduledExecutor,
                                  new FakePostOffice(),
                                  null,
                                  null);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);

      queue.addConsumer(consumer);

      List<MessageReference> refs = new ArrayList<MessageReference>();

      MessageReference ref1 = generateReference(queue, 1);

      ref1.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      queue.addLast(ref1);

      MessageReference ref2 = generateReference(queue, 2);

      ref2.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      queue.addLast(ref2);

      refs.add(ref2);

      assertEquals(2, queue.getMessageCount());

      assertEquals(1, consumer.getReferences().size());

      assertEquals(1, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());

      queue.acknowledge(ref2);

      queue.removeConsumer(consumer);

      queue.addConsumer(consumer);

      queue.deliverNow();

      refs.clear();

      consumer.clearReferences();

      MessageReference ref3 = generateReference(queue, 3);

      ref3.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      queue.addLast(ref3);

      MessageReference ref4 = generateReference(queue, 4);

      ref4.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      queue.addLast(ref4);

      refs.add(ref4);

      assertEquals(3, queue.getMessageCount());

      assertEquals(1, consumer.getReferences().size());

      assertEquals(1, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());
   }

   public void testBusyConsumerWithFilterThenAddMoreMessages() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      FakeConsumer consumer = new FakeConsumer(FilterImpl.createFilter("color = 'green'"));

      consumer.setStatusImmediate(HandleStatus.BUSY);

      queue.addConsumer(consumer);

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);
         ref.getMessage().putStringProperty("color", "red");
         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      queue.deliverNow();

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);
         ref.getMessage().putStringProperty("color", "green");
         queue.addLast(ref);
      }

      assertEquals(20, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      consumer.setStatusImmediate(null);

      for (int i = numMessages * 2; i < numMessages * 3; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }

      queue.deliverNow();

      assertEquals(numMessages, consumer.getReferences().size());
      assertEquals(30, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(10, queue.getDeliveringCount());
   }

   public void testConsumerWithFilterThenAddMoreMessages() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      final int numMessages = 10;
      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);
         ref.getMessage().putStringProperty("color", "red");
         refs.add(ref);

         queue.addLast(ref);
      }

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      queue.deliverNow();

      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      for (int i = numMessages; i < numMessages * 2; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);
         ref.getMessage().putStringProperty("color", "green");
         queue.addLast(ref);
      }

      FakeConsumer consumer = new FakeConsumer(FilterImpl.createFilter("color = 'green'"));

      queue.addConsumer(consumer);

      queue.deliverNow();

      assertEquals(20, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(10, queue.getDeliveringCount());

      for (int i = numMessages * 2; i < numMessages * 3; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);
         ref.getMessage().putStringProperty("color", "green");
         queue.addLast(ref);
      }

      queue.deliverNow();

      assertEquals(20, consumer.getReferences().size());
      assertEquals(30, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(20, queue.getDeliveringCount());
   }

   // Private ------------------------------------------------------------------------------

   private void testConsumerWithFilters(boolean direct) throws Exception
   {
      Queue queue = new QueueImpl(1,
                                  address1,
                                  queue1,
                                  null,
                                  false,
                                  true,
                                  scheduledExecutor,
                                  new FakePostOffice(),
                                  null,
                                  null);

      Filter filter = new FakeFilter("fruit", "orange");

      FakeConsumer consumer = new FakeConsumer(filter);

      if (direct)
      {
         queue.addConsumer(consumer);
      }

      List<MessageReference> refs = new ArrayList<MessageReference>();

      MessageReference ref1 = generateReference(queue, 1);

      ref1.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("banana"));

      queue.addLast(ref1);

      MessageReference ref2 = generateReference(queue, 2);

      ref2.getMessage().putStringProperty(new SimpleString("cheese"), new SimpleString("stilton"));

      queue.addLast(ref2);

      MessageReference ref3 = generateReference(queue, 3);

      ref3.getMessage().putStringProperty(new SimpleString("cake"), new SimpleString("sponge"));

      queue.addLast(ref3);

      MessageReference ref4 = generateReference(queue, 4);

      ref4.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      refs.add(ref4);

      queue.addLast(ref4);

      MessageReference ref5 = generateReference(queue, 5);

      ref5.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("apple"));

      queue.addLast(ref5);

      MessageReference ref6 = generateReference(queue, 6);

      ref6.getMessage().putStringProperty(new SimpleString("fruit"), new SimpleString("orange"));

      refs.add(ref6);

      queue.addLast(ref6);

      if (!direct)
      {
         queue.addConsumer(consumer);

         queue.deliverNow();
      }

      assertEquals(6, queue.getMessageCount());

      assertEquals(2, consumer.getReferences().size());

      assertEquals(2, queue.getDeliveringCount());

      assertRefListsIdenticalRefs(refs, consumer.getReferences());

      queue.acknowledge(ref5);
      queue.acknowledge(ref6);

      queue.removeConsumer(consumer);

      consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      queue.deliverNow();

      assertEquals(4, queue.getMessageCount());

      assertEquals(4, consumer.getReferences().size());

      assertEquals(4, queue.getDeliveringCount());
   }

   public void testMessageOrder() throws Exception
   {
      FakeConsumer consumer = new FakeConsumer();
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addFirst(messageReference);
      queue.addLast(messageReference2);
      queue.addFirst(messageReference3);

      assertEquals(0, consumer.getReferences().size());
      queue.addConsumer(consumer);
      queue.deliverNow();

      assertEquals(3, consumer.getReferences().size());
      assertEquals(messageReference3, consumer.getReferences().get(0));
      assertEquals(messageReference, consumer.getReferences().get(1));
      assertEquals(messageReference2, consumer.getReferences().get(2));
   }

   public void testMessagesAdded() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addLast(messageReference);
      queue.addLast(messageReference2);
      queue.addLast(messageReference3);
      assertEquals(queue.getMessagesAdded(), 3);
   }

   public void testGetReference() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);
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
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);
      MessageReference messageReference = generateReference(queue, 1);
      MessageReference messageReference2 = generateReference(queue, 2);
      MessageReference messageReference3 = generateReference(queue, 3);
      queue.addFirst(messageReference);
      queue.addFirst(messageReference2);
      queue.addFirst(messageReference3);
      assertNull(queue.getReference(5));

   }

   /**
    * Test the paused and resumed states with async deliveries.
    * @throws Exception
    */
   public void testPauseAndResumeWithAsync() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);
      
      // pauses the queue
      queue.pause();

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);

         refs.add(ref);

         queue.addLast(ref);
      }
      // even as this queue is paused, it will receive the messages anyway
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());

      // Now add a consumer
      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      assertTrue(consumer.getReferences().isEmpty());
      assertEquals(10, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      // explicit order of delivery
      queue.deliverNow();
      // As the queue is paused, even an explicit order of delivery will not work.
      assertEquals(0, consumer.getReferences().size());
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      // resuming work
      queue.resume();

      // after resuming the delivery begins.
      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(numMessages, queue.getDeliveringCount());

   }

   /**
    * Test the paused and resumed states with direct deliveries.
    * @throws Exception
    */

   public void testPauseAndResumeWithDirect() throws Exception
   {
      Queue queue = new QueueImpl(1, address1, queue1, null, false, true, scheduledExecutor, null, null, null);

      // Now add a consumer
      FakeConsumer consumer = new FakeConsumer();

      queue.addConsumer(consumer);

      // brings to queue to paused state
      queue.pause();

      final int numMessages = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();

      for (int i = 0; i < numMessages; i++)
      {
         MessageReference ref = generateReference(queue, i);
         refs.add(ref);
         queue.addLast(ref);
      }

      // the queue even if it's paused will receive the message but won't forward
      // directly to the consumer until resumed.
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(0, queue.getScheduledCount());
      assertEquals(0, queue.getDeliveringCount());
      assertTrue(consumer.getReferences().isEmpty());

      // brings the queue to resumed state.
      queue.resume();
      // resuming delivery of messages
      assertRefListsIdenticalRefs(refs, consumer.getReferences());
      assertEquals(numMessages, queue.getMessageCount());
      assertEquals(numMessages, queue.getDeliveringCount());

   }

   class AddtoQueueRunner implements Runnable
   {
      Queue queue;

      MessageReference messageReference;

      boolean added = false;

      CountDownLatch countDownLatch;

      boolean first;

      public AddtoQueueRunner(boolean first,
                              Queue queue,
                              MessageReference messageReference,
                              CountDownLatch countDownLatch)
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

   class DummyDistributionPolicy implements Distributor
   {
      Consumer consumer;

      public List<Consumer> getConsumers()
      {
         return null;
      }

      public void addConsumer(Consumer consumer)
      {
         this.consumer = consumer;
      }

      public boolean removeConsumer(Consumer consumer)
      {
         return false;
      }

      public int getConsumerCount()
      {
         return 0;
      }

      public Consumer getNextConsumer()
      {
         return consumer;
      }
   }

}
