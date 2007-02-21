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
package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultMessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.QueueStats;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;


public class DefaultMessagePullPolicyTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   // Constructors --------------------------------------------------

   public DefaultMessagePullPolicyTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");
      
      sc.start();                
    
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }

      super.tearDown();
   }
   
   public void test1() throws Exception
   {
      MessagePullPolicy policy = new DefaultMessagePullPolicy();
      
      List queues = new ArrayList();
      
      ClusteredQueue queue1 = new DummyClusteredQueue(true, "queue1", 1000);
      
      queues.add(queue1);
      
      ClusteredQueue queue2 = new DummyClusteredQueue(false, "queue2", 435);
      
      queues.add(queue2);
      
      ClusteredQueue queue3 = new DummyClusteredQueue(false, "queue3", 12);
      
      queues.add(queue3);
      
      ClusteredQueue queue4 = new DummyClusteredQueue(false, "queue4", 900);
      
      queues.add(queue4);
      
      ClusteredQueue queue5 = new DummyClusteredQueue(false, "queue5", 0);
      
      queues.add(queue5);
      
      ClusteredQueue chosen = policy.chooseQueue(queues);
      
      assertTrue(chosen == queue4);
   }
   
   public void test2() throws Exception
   {
      MessagePullPolicy policy = new DefaultMessagePullPolicy();
      
      List queues = new ArrayList();
      
      ClusteredQueue queue1 = new DummyClusteredQueue(true, "queue1", 0);
      
      queues.add(queue1);
      
      ClusteredQueue queue2 = new DummyClusteredQueue(false, "queue2", 0);
      
      queues.add(queue2);
      
      ClusteredQueue queue3 = new DummyClusteredQueue(false, "queue3", 0);
      
      queues.add(queue3);
      
      ClusteredQueue queue4 = new DummyClusteredQueue(false, "queue4", 0);
      
      queues.add(queue4);
      
      ClusteredQueue queue5 = new DummyClusteredQueue(false, "queue5", 0);
      
      queues.add(queue5);
      
      ClusteredQueue chosen = policy.chooseQueue(queues);
      
      assertNull(chosen);
   }
   
   public void test3() throws Exception
   {
      MessagePullPolicy policy = new DefaultMessagePullPolicy();
      
      List queues = new ArrayList();
      
      ClusteredQueue queue1 = new DummyClusteredQueue(true, "queue1", 0);
      
      queues.add(queue1);           
      
      ClusteredQueue chosen = policy.chooseQueue(queues);
      
      assertNull(chosen);
   }
   
   public void test4() throws Exception
   {
      MessagePullPolicy policy = new DefaultMessagePullPolicy();
      
      List queues = new ArrayList();               
      
      ClusteredQueue chosen = policy.chooseQueue(queues);
      
      assertNull(chosen);
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class DummyClusteredQueue implements ClusteredQueue
   {
      private boolean local;
      
      private String queueName;
      
      private int msgCount;
      
      DummyClusteredQueue(boolean local, String queueName, int msgCount)
      {
         this.local = local;
         this.queueName = queueName;
         this.msgCount = msgCount;
      }

      public int getNodeId()
      {
         // TODO Auto-generated method stub
         return -1;
      }

      public QueueStats getStats()
      {
         return new QueueStats(queueName, msgCount);
      }

      public boolean isLocal()
      {
         return local;
      }

      public Filter getFilter()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getName()
      {
         return queueName;
      }

      public boolean isClustered()
      {
         return true;
      }

      public void acknowledge(Delivery d, Transaction tx) throws Throwable
      {
         // TODO Auto-generated method stub
         
      }

      public void cancel(Delivery d) throws Throwable
      {
         // TODO Auto-generated method stub
         
      }

      public boolean acceptReliableMessages()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void activate()
      {
         // TODO Auto-generated method stub
         
      }

      public List browse()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public List browse(Filter filter)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void clear()
      {
         // TODO Auto-generated method stub
         
      }

      public void close()
      {
         // TODO Auto-generated method stub
         
      }

      public void deactivate()
      {
         // TODO Auto-generated method stub
         
      }

      public void deliver()
      {
         // TODO Auto-generated method stub
         
      }

      public List delivering(Filter filter)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getChannelID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public boolean isActive()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isRecoverable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void load() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public int getMessageCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void removeAllReferences() throws Throwable
      {
         // TODO Auto-generated method stub
         
      }

      public List undelivered(Filter filter)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void unload() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean add(Receiver receiver)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean contains(Receiver receiver)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public Iterator iterator()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getNumberOfReceivers()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public boolean remove(Receiver receiver)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public List recoverDeliveries(List messageIds)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void addDelivery(Delivery del)
      {
         // TODO Auto-generated method stub
         
      }

      public int getDeliveringCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getMaxSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getMessagesAdded()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void setMaxSize(int newSize)
      {
         // TODO Auto-generated method stub
         
      }

      public int getScheduledCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }
      
   }

}


