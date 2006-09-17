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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.BasicRedistributionPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusteredPostOfficeImpl;
import org.jboss.messaging.core.plugin.postoffice.cluster.FavourLocalRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.RedistributionPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.RouterFactory;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.postoffice.SimpleFilter;
import org.jboss.test.messaging.core.plugin.postoffice.SimpleFilterFactory;
import org.jboss.test.messaging.core.plugin.postoffice.SimplePostOfficeTest;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A ClusteredPostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredPostOfficeTest extends SimplePostOfficeTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClusteredPostOfficeTest(String name)
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
   
   public final void testClusteredBindUnbind() throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      try
      {         
         //Start one office
         
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         //Add a couple of bindings
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue("node1", "sub1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 =
            office1.bindClusteredQueue("topic1", queue1);
         LocalClusteredQueue queue2 = new LocalClusteredQueue("node1", "sub2", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);

         Binding binding2 =
            office1.bindClusteredQueue("topic1", queue2);
         
         //Start another office - make sure it picks up the bindings from the first node
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         Collection bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         Iterator iter = bindings.iterator();
         assertEquivalent(binding1, (Binding)iter.next());
         assertEquivalent(binding2, (Binding)iter.next());         
         
         //Add another binding on node 2
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue("node2", "sub3", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);

         Binding binding3 =
            office2.bindClusteredQueue("topic1", queue3);
  
         //Make sure both nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding1, (Binding)iter.next());
         assertEquivalent(binding2, (Binding)iter.next());
         assertEquivalent(binding3, (Binding)iter.next());

         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding1, (Binding)iter.next());
         assertEquivalent(binding2, (Binding)iter.next());
         assertEquivalent(binding3, (Binding)iter.next());

         //Add another binding on node 1
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue("node2", "sub4", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         Binding binding4 =
            office2.bindClusteredQueue("topic1", queue4);
         
         // Make sure both nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding1, (Binding)iter.next());
         assertEquivalent(binding2, (Binding)iter.next());
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding1, (Binding)iter.next());
         assertEquivalent(binding2, (Binding)iter.next());
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         
         //Unbind binding 1 and binding 2
         office1.unbindClusteredQueue("sub1");
         office1.unbindClusteredQueue("sub2");
         
         //Make sure bindings are not longer available on either node
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
   
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         
         //Add a third office
                  
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         //Maks sure it picks up the bindings
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         
         //Add another binding on node 3
                  
         LocalClusteredQueue queue5 = new LocalClusteredQueue("node3", "sub5", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding5 =
            office3.bindClusteredQueue("topic1", queue5);
         
         // Make sure all nodes pick it up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(3, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         
         //Add a durable and a non durable binding on node 1
         
         LocalClusteredQueue queue6 = new LocalClusteredQueue("node1", "sub6", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         
         Binding binding6 =
            office1.bindClusteredQueue("topic1", queue6);
         
         LocalClusteredQueue queue7 = new LocalClusteredQueue("node1", "sub7", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         
         Binding binding7 =
            office1.bindClusteredQueue("topic1", queue7);
         
         // Make sure all nodes pick them up
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         assertEquivalent(binding7, (Binding)iter.next());
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         assertEquivalent(binding7, (Binding)iter.next());
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(5, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         assertEquivalent(binding7, (Binding)iter.next());
               
         //Stop office 1
         office1.stop();
  
         //Need to sleep since it may take some time for the view changed request to reach the
         //members which causes the bindings to be removed
         
         Thread.sleep(1000);
         
         //All it's non durable bindings should be removed from the other nodes
         //Durable bindings should remain
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(4, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding3, (Binding)iter.next());
         assertEquivalent(binding4, (Binding)iter.next());
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         //Stop office 2
         office2.stop();
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         //Restart office 1 and office 2
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(2, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding5, (Binding)iter.next());
         assertEquivalent(binding6, (Binding)iter.next());
         
         //Stop all offices
         
         office1.stop();
         office2.stop();
         office3.stop();
         
         //Start them all
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
         office3 = createClusteredPostOffice("node3", "testgroup");
         
         //Only the durable queue should survive
         
         bindings = office1.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding6, (Binding)iter.next());
         
         bindings = office2.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding6, (Binding)iter.next());
         
         bindings = office3.listBindingsForCondition("topic1");
         assertNotNull(bindings);
         assertEquals(1, bindings.size());
         
         iter = bindings.iterator();
         assertEquivalent(binding6, (Binding)iter.next());
                  
      }
      finally
      {
         if (office1 != null)
         {
            office1.unbindClusteredQueue("sub6");
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
         
         if (office3 != null)
         {
            office2.stop();
         }
      }
      
   }
   
   public final void testClusteredRoutePersistent() throws Throwable
   {
      clusteredRoute(true);
   }
   
   public final void testClusteredRouteNonPersistent() throws Throwable
   {
      clusteredRoute(false);
   }
   
   public final void testClusteredTransactionalRoutePersistent() throws Throwable
   {
      clusteredTransactionalRoute(true);
   }
   
   public final void testClusteredTransactionalRouteNonPersistent() throws Throwable
   {
      clusteredTransactionalRoute(false);
   }
   
   public void testClusteredNonPersistentRouteWithFilter() throws Throwable
   {
      this.clusteredRouteWithFilter(false);
   }
   
   public void testClusteredPersistentRouteWithFilter() throws Throwable
   {
      this.clusteredRouteWithFilter(true);
   }
   
   
   /*
    * We should allow the clustered bind of queues with the same queue name on different nodes of the
    * cluster
    */
   public void testClusteredNameUniqueness() throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue("node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 = office1.bindClusteredQueue("queue1", queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue("node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding2 = office2.bindClusteredQueue("queue1", queue2);
                  
         LocalClusteredQueue queue3 = new LocalClusteredQueue("node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         try
         {
            Binding binding3 = office1.bindClusteredQueue("queue1", queue3);
            fail();
         }
         catch (Exception e)
         {
            //Ok
         }
         LocalClusteredQueue queue4 = new LocalClusteredQueue("node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         try
         {
            Binding binding4 = office2.bindClusteredQueue("queue1", queue4);
            fail();
         }
         catch (Exception e)
         {
            //Ok
         }
         
         office1.unbindClusteredQueue("queue1");
         
         office2.unbindClusteredQueue("queue1");
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue("node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding5 = office1.bindClusteredQueue("queue1", queue5);
         
         PagingFilteredQueue queue6 = new PagingFilteredQueue("queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);       
         try
         {
            Binding binding6 = office1.bindQueue("queue1", queue6);
            fail();
         }
         catch (Exception e)
         {
            //ok
         }
         
         try
         {
            office1.unbindQueue("queue1");
            fail();
         }
         catch (Exception e)
         {
            //ok
         }
         
         office1.unbindClusteredQueue("queue1");
         
         PagingFilteredQueue queue7 = new PagingFilteredQueue("queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);       
         Binding binding7 = office1.bindQueue("queue1", queue7);
         
         PagingFilteredQueue queue8 = new PagingFilteredQueue("queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);       
         Binding binding8 = office2.bindQueue("queue1", queue8);
         
         PagingFilteredQueue queue9 = new PagingFilteredQueue("queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);    
         try
         {
            Binding binding9 = office1.bindQueue("queue1", queue9);
            fail();
         }
         catch (Exception e)
         {
            //Ok
         }

      }
      finally
      {
         if (office1 != null)
         {            
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void clusteredRouteWithFilter(boolean persistentMessage) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         SimpleFilter filter1 = new SimpleFilter(2);
         SimpleFilter filter2 = new SimpleFilter(3);
      
         LocalClusteredQueue queue1 = new LocalClusteredQueue("node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), filter1);         
         Binding binding1 =
            office1.bindClusteredQueue("topic1", queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue("node2", "queue2", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), filter2);         
         Binding binding2 =
            office2.bindClusteredQueue("topic1", queue2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue("node3", "queue3", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         Binding binding3 =
            office2.bindClusteredQueue("topic1", queue3);   
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);         
         boolean routed = office1.route(ref1, "topic1", null);      
         assertTrue(routed);
         Message msg2 = CoreMessageFactory.createCoreMessage(2);      
         MessageReference ref2 = ms.reference(msg2);         
         routed = office1.route(ref2, "topic1", null);      
         assertTrue(routed);
         Message msg3 = CoreMessageFactory.createCoreMessage(3);      
         MessageReference ref3 = ms.reference(msg3);         
         routed = office1.route(ref3, "topic1", null);      
         assertTrue(routed);
         
         List msgs = receiver1.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         Message msgRec = (Message)msgs.get(0);
         assertTrue(msg2 == msgRec);
         receiver1.acknowledge(msgRec, null);
         msgs = queue1.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver2.getMessages();
         assertNotNull(msgs);
         assertEquals(1, msgs.size());
         msgRec = (Message)msgs.get(0);
         assertTrue(msg3 == msgRec);
         receiver2.acknowledge(msgRec, null);
         msgs = queue2.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty());  
         
         msgs = receiver3.getMessages();
         assertNotNull(msgs);
         assertEquals(3, msgs.size());
         Message msgRec1 = (Message)msgs.get(0);
         assertTrue(msg1 == msgRec1);
         Message msgRec2 = (Message)msgs.get(1);
         assertTrue(msg2 == msgRec2);
         Message msgRec3 = (Message)msgs.get(2);
         assertTrue(msg3 == msgRec3);
          
         receiver3.acknowledge(msgRec1, null);
         receiver3.acknowledge(msgRec2, null);
         receiver3.acknowledge(msgRec3, null);
         msgs = queue3.browse();
         assertNotNull(msgs);
         assertTrue(msgs.isEmpty()); 
         
      }
      finally
      {
         if (office1 != null)
         {
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   protected void clusteredRoute(boolean persistentMessage) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
      
         //Two topics with a mixture of durable and non durable subscriptions
         
         LocalClusteredQueue[] queues = new LocalClusteredQueue[16];
         Binding[] bindings = new Binding[16];
         
         queues[0] = new LocalClusteredQueue("node1", "sub1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[0] = office1.bindClusteredQueue("topic1", queues[0]);
         
         queues[1] = new LocalClusteredQueue("node1", "sub2", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[1] = office1.bindClusteredQueue("topic1", queues[1]);
         
         queues[2] = new LocalClusteredQueue("node2", "sub3", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[2] = office2.bindClusteredQueue("topic1", queues[2]);
         
         queues[3] = new LocalClusteredQueue("node2", "sub4", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[3] = office2.bindClusteredQueue("topic1", queues[3]);
         
         queues[4] = new LocalClusteredQueue("node2", "sub5", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[4] = office2.bindClusteredQueue("topic1", queues[4]);
         
         queues[5] = new LocalClusteredQueue("node1", "sub6", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[5] = office1.bindClusteredQueue("topic1", queues[5]);
         
         queues[6] = new LocalClusteredQueue("node1", "sub7", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[6] = office1.bindClusteredQueue("topic1", queues[6]);
         
         queues[7] = new LocalClusteredQueue("node1", "sub8", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[7] = office1.bindClusteredQueue("topic1", queues[7]);
         
         queues[8] = new LocalClusteredQueue("node2", "sub9", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[8] = office1.bindClusteredQueue("topic2", queues[8]);
         
         queues[9] = new LocalClusteredQueue("node2", "sub10", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[9] = office1.bindClusteredQueue("topic2", queues[9]);
         
         queues[10] = new LocalClusteredQueue("node2", "sub11", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[10] = office2.bindClusteredQueue("topic2", queues[10]);
         
         queues[11] = new LocalClusteredQueue("node2", "sub12", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[11] = office2.bindClusteredQueue("topic2", queues[11]);
         
         queues[12] = new LocalClusteredQueue("node2", "sub13", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[12] = office2.bindClusteredQueue("topic2", queues[12]);
         
         queues[13] = new LocalClusteredQueue("node1", "sub14", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[13] = office1.bindClusteredQueue("topic2", queues[13]);
         
         queues[14] = new LocalClusteredQueue("node1", "sub15", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[14] = office1.bindClusteredQueue("topic2", queues[14]);
         
         queues[15] = new LocalClusteredQueue("node1", "sub16", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[15] = office1.bindClusteredQueue("topic2", queues[15]);
       
         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         Message msg = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
         MessageReference ref = ms.reference(msg);         

         boolean routed = office1.route(ref, "topic1", null);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(1, msgs.size());
            Message msgRec = (Message)msgs.get(0);
            assertEquals(msg.getMessageID(), msgRec.getMessageID());
            receivers[i].acknowledge(msgRec, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
                  
         //Now route to topic2
         
         msg = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);;      
         ref = ms.reference(msg);         

         routed = office1.route(ref, "topic2", null);         
         assertTrue(routed);
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(1, msgs.size());
            Message msgRec = (Message)msgs.get(0);
            assertEquals(msg.getMessageID(), msgRec.getMessageID());
            receivers[i].acknowledge(msgRec, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }

      }
      finally
      {
         if (office1 != null)
         {
            office2.unbindClusteredQueue("sub5");
            office1.unbindClusteredQueue("sub7");
            office1.unbindClusteredQueue("sub8");
            office2.unbindClusteredQueue("sub13");
            office1.unbindClusteredQueue("sub15");
            office1.unbindClusteredQueue("sub16");
            
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   
   protected void clusteredTransactionalRoute(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      try
      {   
         //Start two offices
         
         office1 = createClusteredPostOffice("node1", "testgroup");
         office2 = createClusteredPostOffice("node2", "testgroup");
     
         LocalClusteredQueue[] queues = new LocalClusteredQueue[16];
         Binding[] bindings = new Binding[16];
         
         queues[0] = new LocalClusteredQueue("node1", "sub1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[0] = office1.bindClusteredQueue("topic1", queues[0]);
         
         queues[1] = new LocalClusteredQueue("node1", "sub2", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[1] = office1.bindClusteredQueue("topic1", queues[1]);
         
         queues[2] = new LocalClusteredQueue("node2", "sub3", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[2] = office2.bindClusteredQueue("topic1", queues[2]);
         
         queues[3] = new LocalClusteredQueue("node2", "sub4", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[3] = office2.bindClusteredQueue("topic1", queues[3]);
         
         queues[4] = new LocalClusteredQueue("node2", "sub5", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[4] = office2.bindClusteredQueue("topic1", queues[4]);
         
         queues[5] = new LocalClusteredQueue("node1", "sub6", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[5] = office1.bindClusteredQueue("topic1", queues[5]);
         
         queues[6] = new LocalClusteredQueue("node1", "sub7", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[6] = office1.bindClusteredQueue("topic1", queues[6]);
         
         queues[7] = new LocalClusteredQueue("node1", "sub8", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[7] = office1.bindClusteredQueue("topic1", queues[7]);
         
         queues[8] = new LocalClusteredQueue("node1", "sub9", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[8] = office1.bindClusteredQueue("topic2", queues[8]);
         
         queues[9] = new LocalClusteredQueue("node1", "sub10", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[9] = office1.bindClusteredQueue("topic2", queues[9]);
         
         queues[10] = new LocalClusteredQueue("node2", "sub11", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[10] = office2.bindClusteredQueue("topic2", queues[10]);
         
         queues[11] = new LocalClusteredQueue("node2", "sub12", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[11] = office2.bindClusteredQueue("topic2", queues[11]);
         
         queues[12] = new LocalClusteredQueue("node2", "sub13", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[12] = office2.bindClusteredQueue("topic2", queues[12]);
         
         queues[13] = new LocalClusteredQueue("node1", "sub14", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);         
         bindings[13] = office1.bindClusteredQueue("topic2", queues[13]);
         
         queues[14] = new LocalClusteredQueue("node1", "sub15", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[14] = office1.bindClusteredQueue("topic2", queues[14]);
         
         queues[15] = new LocalClusteredQueue("node1", "sub16", im.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null);         
         bindings[15] = office1.bindClusteredQueue("topic2", queues[15]);

         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
            queues[i].add(receivers[i]);
         }
         
         //First for topic 1
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();

         boolean routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);

         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);            
            assertTrue(msgs.isEmpty());                        
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);         
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         //Now send some non transactionally
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", null);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);         
         
         //And acknowledge transactionally
         
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // and the rollback
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic1", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
                 
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 8; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                                 
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
                           
            receivers[i].clear();
         }
         
         for (int i = 8; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // Now for topic 2
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);    
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);     
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic2", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", tx);         
         assertTrue(routed);
         
         
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(1000);
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty()); 
            receivers[i].clear();
         }
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, "topic1", tx);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic1", tx);         
         assertTrue(routed);
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         //Now send some non transactionally
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
         
         //And acknowledge transactionally
         
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                        
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
                        
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].clear();
         }
         
         
         
         tx.commit();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // and the rollback
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         routed = office1.route(ref1, "topic2", null);         
         assertTrue(routed);
         routed = office1.route(ref2, "topic2", null);         
         assertTrue(routed);
         
         Thread.sleep(1000);
          
         tx = tr.createTransaction();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
            
            
            receivers[i].acknowledge(msgRec1, tx);
            receivers[i].acknowledge(msgRec2, tx);
            
            
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
         }
         
         
         
         tx.rollback();
         
         for (int i = 0; i < 8; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         for (int i = 8; i < 16; i++)
         {         
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            Message msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            Message msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());      
                                              
            msgs = queues[i].browse();
            
            assertNotNull(msgs);
            assertEquals(2, msgs.size());
            msgRec1 = (Message)msgs.get(0);
            assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
            msgRec2 = (Message)msgs.get(1);
            assertEquals(msg2.getMessageID(), msgRec2.getMessageID());
            
            receivers[i].acknowledge(msgRec1, null);
            receivers[i].acknowledge(msgRec2, null);             
            
            receivers[i].clear();
         }
      }
      finally
      {
         if (office1 != null)
         {
            office2.unbindClusteredQueue("sub5");
            office1.unbindClusteredQueue("sub7");
            office1.unbindClusteredQueue("sub8");
            office2.unbindClusteredQueue("sub13");
            office1.unbindClusteredQueue("sub15");
            office1.unbindClusteredQueue("sub16");
            office1.stop();
         }
         
         if (office2 != null)
         {
            office2.stop();
         }
      }
   }
   
   protected ClusteredPostOffice createClusteredPostOffice(String nodeId, String groupName) throws Exception
   {
      RedistributionPolicy redistPolicy = new NullRedistributionPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      RouterFactory rf = new FavourLocalRouterFactory();
      
      ClusteredPostOfficeImpl postOffice = 
         new ClusteredPostOfficeImpl(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(50, 1),
                                 JGroupsUtil.getDataStackProperties(50, 1),
                                 5000, 5000, redistPolicy, 1000, rf);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



