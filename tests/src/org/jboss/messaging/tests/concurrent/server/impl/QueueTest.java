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

package org.jboss.messaging.tests.concurrent.server.impl;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

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

   
   private QueueFactory queueFactory = new FakeQueueFactory();
   
   /*
    * Concurrent set consumer not busy, busy then, call deliver while messages are being added and consumed
    */
   public void testConcurrentAddsDeliver() throws Exception
   {
      Queue queue = queueFactory.createQueue(1, new SimpleString("queue1"), null, false, true);
      
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
      
      queue.deliver();
      
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
               
               queue.deliver();
            }
            toggle = !toggle;
            
            numToggles++;
         }
      }
   }
      
}



