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
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouter;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouter;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.NullMessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.QueueStats;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.ClusteringTestBase;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * 
 * A DefaultRouterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultRouterTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public DefaultRouterTest(String name)
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
   
   public void testSize() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();
      
      ClusteredQueue queue1 = new SimpleQueue(true);
      dr.add(queue1);
      
      assertEquals(1, dr.numberOfReceivers());
      assertEquals(1, dr.getQueues().size());
      
      ClusteredQueue queue2 = new SimpleQueue(false);
      dr.add(queue2);
      
      assertEquals(2, dr.numberOfReceivers());
      assertEquals(2, dr.getQueues().size());
      
      ClusteredQueue queue3 = new SimpleQueue(false);
      dr.add(queue3);
      
      assertEquals(3, dr.numberOfReceivers());
      assertEquals(3, dr.getQueues().size());
      
      dr.remove(queue3);
      
      assertEquals(2, dr.numberOfReceivers());
      assertEquals(2, dr.getQueues().size());
      
      dr.remove(queue2);
      
      assertEquals(1, dr.numberOfReceivers());
      assertEquals(1, dr.getQueues().size());
      
      dr.remove(queue1);
      
      assertEquals(0, dr.numberOfReceivers());
      assertTrue(dr.getQueues().isEmpty());
      
   }
   
   // The router only has a local queue
   public void testRouterOnlyLocal() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();
                    
      ClusteredQueue queue = new SimpleQueue(true);
      
      SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      queue.add(receiver1);
               
      dr.add(queue);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver1);
   }
   
   //The router has only one non local queues
   public void testRouterOnlyOneNonLocal() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();
                    
      ClusteredQueue queue = new SimpleQueue(false);
      
      SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      queue.add(receiver1);
      
      dr.add(queue);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver1);              
   }
   
   //The router has multiple non local queues and no local queue
   public void testRouterMultipleNonLocal() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();
                   
      ClusteredQueue remote1 = new SimpleQueue(false);
     
      SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote1.add(receiver1);
      
      dr.add(remote1);
      
      
      ClusteredQueue remote2 = new SimpleQueue(false);
      
      SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote2.add(receiver2);
      
      dr.add(remote2);
      
      
      ClusteredQueue remote3 = new SimpleQueue(false);
      
      SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote3.add(receiver3);
      
      dr.add(remote3);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver2);
      
      sendAndCheck(dr, receiver3);
      
      sendAndCheck(dr, receiver1);
      
      sendAndCheck(dr, receiver2);
      
      sendAndCheck(dr, receiver3);
   }
   
   
   // The router has one local with consumer and one non local queue with consumer
   public void testRouterOneLocalOneNonLocal() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();
                             
      ClusteredQueue remote1 = new SimpleQueue(false);
     
      SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote1.add(receiver1);
      
      dr.add(remote1);
      
      ClusteredQueue queue = new SimpleQueue(true);
      
      SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      queue.add(receiver2);
      
      dr.add(queue);

      sendAndCheck(dr, receiver2);
      
      sendAndCheck(dr, receiver2);
      
      sendAndCheck(dr, receiver2);                  
   }
   
   // The router has multiple non local queues with consumers and one local queue
   public void testRouterMultipleNonLocalOneLocal() throws Exception
   {
      DefaultRouter dr = new DefaultRouter();            
                  
      ClusteredQueue remote1 = new SimpleQueue(false);
      
      SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote1.add(receiver1);
      
      dr.add(remote1);
      
      
      ClusteredQueue remote2 = new SimpleQueue(false);
      
      SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote2.add(receiver2);
      
      dr.add(remote2);
      
      
      ClusteredQueue remote3 = new SimpleQueue(false);
      
      SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      remote3.add(receiver3);
      
      dr.add(remote3);
      
      
      ClusteredQueue queue = new SimpleQueue(true);
            
      SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
      
      queue.add(receiver4);
      
      dr.add(queue);
      
      
      sendAndCheck(dr, receiver4);
      
      sendAndCheck(dr, receiver4);
      
      sendAndCheck(dr, receiver4);
   }
   
   private long nextId;
   
   private void sendAndCheck(ClusterRouter router, SimpleReceiver receiver) throws Exception
   {
      Message msg = CoreMessageFactory.createCoreMessage(nextId++, false, null);      
      
      MessageReference ref = ms.reference(msg);         
      
      Delivery del = router.handle(null, ref, null);
      
      assertNotNull(del);
      
      assertTrue(del.isSelectorAccepted());
            
      Thread.sleep(250);
      
      List msgs = receiver.getMessages();
      
      assertNotNull(msgs);
      
      assertEquals(1, msgs.size());
      
      Message msgRec = (Message)msgs.get(0);
      
      assertTrue(msg == msgRec);  
      
      receiver.clear();
   }
   
   private void sendAndCheck(ClusterRouter router, Queue queue) throws Throwable
   {
      Message msg = CoreMessageFactory.createCoreMessage(nextId++, false, null);      
      
      MessageReference ref = ms.reference(msg);         
      
      Delivery del = router.handle(null, ref, null);
      
      assertNotNull(del);
      
      assertTrue(del.isSelectorAccepted());
            
      Thread.sleep(250);
      
      List msgs = queue.browse();
      
      assertNotNull(msgs);
      
      assertEquals(1, msgs.size());
      
      Message msgRec = (Message)msgs.get(0);
      
      assertTrue(msg == msgRec);  
      
      queue.removeAllReferences();
   }
   
   
   
   protected ClusteredPostOffice createClusteredPostOffice(int nodeId, String groupName) throws Exception
   {
      MessagePullPolicy redistPolicy = new NullMessagePullPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new DefaultRouterFactory();
      
      DefaultClusteredPostOffice postOffice = 
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 5000, 5000, redistPolicy, rf, 1, 1000);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------
   
   
   // Inner classes -------------------------------------------------
   
   class SimpleQueue implements ClusteredQueue
   {
      private boolean local;
      
      private Receiver receiver;
      
      private List refs = new ArrayList();
        
      SimpleQueue(boolean local)
      {
         this.local = local;
      }

      public int getNodeId()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public QueueStats getStats()
      {
         // TODO Auto-generated method stub
         return null;
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
         // TODO Auto-generated method stub
         return null;
      }

      public boolean isClustered()
      {
         // TODO Auto-generated method stub
         return false;
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
         List msgs = new ArrayList();
         
         Iterator iter = refs.iterator();
         
         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference)iter.next();
            
            msgs.add(ref);
         }
         
         return msgs;
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

      public void deliver(boolean synchronous)
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

      public int messageCount()
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
         if (receiver != null)
         {
            //Send to receiver
            
            Delivery del = receiver.handle(observer, reference, tx);
            
            return del;
         }
         else
         {
            //Store internally
            refs.add(reference);
            
            return new SimpleDelivery(observer, reference);
         }
         
      
      }

      public void acknowledge(Delivery d, Transaction tx) throws Throwable
      {
         // TODO Auto-generated method stub
         
      }

      public void cancel(Delivery d) throws Throwable
      {
         // TODO Auto-generated method stub
         
      }

      public boolean add(Receiver receiver)
      {
         this.receiver = receiver;
         
         return true;
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

      public int numberOfReceivers()
      {
         if (receiver != null)
         {
            return 1;
         }
         else
         {
            return 0;
         }
      }

      public boolean remove(Receiver receiver)
      {
         // TODO Auto-generated method stub
         return false;
      }
      
   }
   

}



