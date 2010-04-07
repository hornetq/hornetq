/*
 * Copyright 2010 Red Hat, Inc.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.hornetq.tests.util.UnitTestCase;

public class QueueImplPriorityTest extends UnitTestCase
{
   // The tests ----------------------------------------------------------------

   private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

   private static final SimpleString queue1 = new SimpleString("queue1");

   private static final SimpleString address1 = new SimpleString("address1");

   class FakeFilter implements Filter
   {
      public SimpleString getFilterString()
      {
         return null;
      }

      public boolean match(final ServerMessage message)
      {
         return true;
      }

   }

   public void testPriority() throws Exception
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();

      FakeConsumer cons1 = new FakeConsumer(new FakeFilter());

      QueueImpl queue = new QueueImpl(1, address1, queue1, null, // filter
                                      false, // durable
                                      true, // temporary
                                      scheduledExecutor,
                                      null, // post office
                                      null, // storage manager
                                      null, // address setting repo
                                      executor); // executor

      queue.addConsumer(cons1);

      cons1.setStatusImmediate(HandleStatus.HANDLED);
      MessageReference ref = generateReference(queue, 0);
      ref.getMessage().setPriority((byte)2);
      queue.addLast(ref);

      ref = generateReference(queue, 1);
      ref.getMessage().setPriority((byte)2);
      queue.addLast(ref);

      Assert.assertEquals(2, queue.getMessageCount());
      Assert.assertEquals(2, queue.getDeliveringCount());
      Assert.assertEquals(2, cons1.getReferences().size());

      cons1.setStatusImmediate(HandleStatus.BUSY);
      ref = generateReference(queue, 3);
      ref.getMessage().setPriority((byte)2);
      queue.addLast(ref);

      ref = generateReference(queue, 4);
      ref.getMessage().setPriority((byte)2);
      queue.addLast(ref);

      // This will initiate the priority queue iterator, which has 2 elements (msg 3,4)
      queue.deliverNow();

      Assert.assertEquals(4, queue.getMessageCount());
      Assert.assertEquals(2, queue.getDeliveringCount());
      Assert.assertEquals(2, cons1.getReferences().size());

      cons1.setStatusImmediate(HandleStatus.HANDLED);
      ref = generateReference(queue, 2);
      ref.getMessage().setPriority((byte)4);
      queue.addLast(ref);

      ref = generateReference(queue, 5);
      ref.getMessage().setPriority((byte)2);
      queue.addLast(ref);

      Assert.assertEquals(6, queue.getMessageCount());
      Assert.assertEquals(2, queue.getDeliveringCount());
      Assert.assertEquals(2, cons1.getReferences().size());

      // Since the iterator is already initiated and there are more messages with lower priority
      // It will deliver low priority messages first
      queue.deliverNow();

      Assert.assertEquals(6, queue.getMessageCount());
      Assert.assertEquals(6, queue.getDeliveringCount());
      Assert.assertEquals(6, cons1.getReferences().size());

      for (int i = 0; i < 6; i++)
      {
         // System.out.println(cons1.getReferences().get(i).getMessage().getMessageID());
         Assert.assertEquals(i, cons1.getReferences().get(i).getMessage().getMessageID());
      }
      
      executor.shutdown();
   }
}
