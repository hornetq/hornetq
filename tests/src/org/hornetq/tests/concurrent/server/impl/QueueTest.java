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

package org.hornetq.tests.concurrent.server.impl;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.hornetq.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A concurrent QueueTest
 * 
 * All the concurrent queue tests go in here 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class QueueTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(QueueTest.class);
   
   private FakeQueueFactory queueFactory = new FakeQueueFactory();
   
   protected void setUp() throws Exception
   {
      super.setUp();
      queueFactory = new FakeQueueFactory();
   }
   
   protected void tearDown() throws Exception
   {
      queueFactory.stop();
      super.tearDown();
   }
   
   /*
    * Concurrent set consumer not busy, busy then, call deliver while messages are being added and consumed
    */
   public void testConcurrentAddsDeliver() throws Exception
   {
      Queue queue = queueFactory.createQueue(1, new SimpleString("address1"), new SimpleString("queue1"), null, false, false);
      
      FakeConsumer consumer = new FakeConsumer();
      
      queue.addConsumer(consumer);
      
      final long testTime = 5000;
      
      Sender sender = new Sender(queue, testTime);
      
      Toggler toggler = new Toggler(queue, consumer, testTime);
      
      sender.start();
      
      toggler.start();
      
      sender.join();
      
      toggler.join();
      
      consumer.setStatusImmediate(HandleStatus.HANDLED);
      
      queue.deliverNow();

      if (sender.getException() != null)
      {
         throw sender.getException();
      }
      
      if (toggler.getException() != null)
      {
         throw toggler.getException();
      }
      
      assertRefListsIdenticalRefs(sender.getReferences(), consumer.getReferences());
      
      log.info("num refs: " + sender.getReferences().size());
      
      log.info("num toggles: " + toggler.getNumToggles());
      
   }
   
   // Inner classes ---------------------------------------------------------------
   
   class Sender extends Thread
   {
      private volatile Exception e;
      
      private Queue queue;
      
      private long testTime;
      
      private volatile int i;
      
      public Exception getException()
      {
         return e;
      }
      
      private List<MessageReference> refs = new ArrayList<MessageReference>();
      
      public List<MessageReference> getReferences()
      {
         return refs;
      }
      
      Sender(Queue queue, long testTime)
      {
         this.testTime = testTime;
         
         this.queue = queue;
      }
      
      public void run()
      {
         long start = System.currentTimeMillis();
         
         while (System.currentTimeMillis() - start < testTime)
         {
            ServerMessage message = generateMessage(i);
            
            MessageReference ref = message.createReference(queue);
            
            queue.addLast(ref);
            
            refs.add(ref);
            
            i++;
         }
      }
   }
   
   class Toggler extends Thread
   {
      private volatile Exception e;
      
      private Queue queue;
      
      private FakeConsumer consumer;
      
      private long testTime;
      
      private boolean toggle;
      
      private volatile int numToggles;
      
      public int getNumToggles()
      {
         return numToggles;
      }
      
      public Exception getException()
      {
         return e;
      }
      
      Toggler(Queue queue, FakeConsumer consumer, long testTime)
      {
         this.testTime = testTime;
         
         this.queue = queue;
         
         this.consumer = consumer;
      }
      
      public void run()
      {
         long start = System.currentTimeMillis();
         
         while (System.currentTimeMillis() - start < testTime)
         {
            if (toggle)
            {
               consumer.setStatusImmediate(HandleStatus.BUSY);              
            }
            else
            {
               consumer.setStatusImmediate(HandleStatus.HANDLED);
               
               queue.deliverNow();
            }
            toggle = !toggle;
            
            numToggles++;
         }
      }
   }
      
}



