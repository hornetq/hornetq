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
package org.jboss.test.messaging.core.postoffice;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.test.messaging.core.PostOfficeTestBase;
import org.jboss.test.messaging.core.SimpleCondition;
import org.jboss.test.messaging.core.SimpleFilter;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * 
 * A DefaultClusteredPostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2386 $</tt>
 *
 * $Id: DefaultPostOfficeTest.java 2386 2007-02-21 18:07:44Z timfox $
 *
 */
public class ClusteredPostOfficeTest extends PostOfficeTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
    
   // Constructors ---------------------------------------------------------------------------------

   public ClusteredPostOfficeTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public final void testSimpleJoinLeave() throws Throwable
   {
      log.info("testSimpleJoinLeave starts here");
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {         
         office1 = createClusteredPostOffice(1);

         office2 = createClusteredPostOffice(2);
                  
         office3 = createClusteredPostOffice(3);
         
         Thread.sleep(3000);
         
         Set nodes = office1.nodeIDView();         
         assertTrue(nodes.contains(new Integer(1)));
         assertTrue(nodes.contains(new Integer(2)));
         assertTrue(nodes.contains(new Integer(3)));
         
         nodes = office2.nodeIDView();         
         assertTrue(nodes.contains(new Integer(1)));
         assertTrue(nodes.contains(new Integer(2)));
         assertTrue(nodes.contains(new Integer(3)));
         
         nodes = office3.nodeIDView();         
         assertTrue(nodes.contains(new Integer(1)));
         assertTrue(nodes.contains(new Integer(2)));
         assertTrue(nodes.contains(new Integer(3)));
         
         office1.stop();
         office1 = null;
         
         office2.stop();
         office2 = null;
         
         office3.stop();
         office3 = null;
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
         
         if (office3 != null)
         {
            office3.stop();
         }         
      }
         
   }
   
   public final void testGetFailoverMap() throws Throwable
   {
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {         
         office1 = createClusteredPostOffice(1);

         office2 = createClusteredPostOffice(2);
                  
         office3 = createClusteredPostOffice(3);
         
         Thread.sleep(3000);
         
         Map failoverMap1 = office1.getFailoverMap();
         
         Map failoverMap2 = office2.getFailoverMap();
         
         Map failoverMap3 = office3.getFailoverMap();
         
         assertEquals(failoverMap1, failoverMap2);
         
         assertEquals(failoverMap2, failoverMap3);
         
         office1.stop();
         office1 = null;
         
         office2.stop();
         office2 = null;
         
         office3.stop();
         office3 = null;
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
         
         if (office3 != null)
         {
            office3.stop();
         }         
      }
         
   }
   
   public final void testClusteredBindUnbind() throws Throwable
   {
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {         
         // Start one office
         
         office1 = createClusteredPostOffice(1);
          
         // Add a couple of queues
         
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         boolean added = office1.addBinding(new Binding(condition1, queue1, false), false);
         assertTrue(added);
               
         Queue queue2 = new MessagingQueue(1, "sub2", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue2.activate();

         added = office1.addBinding(new Binding(condition1, queue2, false), false);
         assertTrue(added);
               
         // Start another office - make sure it picks up the bindings from the first node
         
         office2 = createClusteredPostOffice(2);
           
         // Should return all queues
         Collection queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
                
         
         // Add another queue on node 2
         
         Queue queue3 = new MessagingQueue(2, "sub3", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue3.activate();

         added = office2.addBinding(new Binding(condition1, queue3, false), false);
         assertTrue(added);
  
         // Make sure both nodes pick it up
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
        

         // Add another binding on node 2
         
         Queue queue4 = new MessagingQueue(2, "sub4", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue4.activate();

         added = office2.addBinding(new Binding(condition1, queue4, false), false);
         assertTrue(added);
         
         // Make sure both nodes pick it up
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(4, queues.size());
         assertTrue(queues.contains(queue1));
         assertTrue(queues.contains(queue2));
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         // Unbind binding 1 and binding 2
         Binding removed = office1.removeBinding(queue1.getName(), false);
         assertNotNull(removed);
                  
         removed = office1.removeBinding(queue2.getName(), false);
         assertNotNull(removed);
         
         // Make sure bindings are not longer available on either node
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         
         // Add a third office
                  
         office3 = createClusteredPostOffice(3);
         
         // Maks sure it picks up the bindings
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         
         // Add another binding on node 3
                  
         Queue queue5 = new MessagingQueue(3, "sub5", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue5.activate();
         
         added = office3.addBinding(new Binding(condition1, queue5, false), false);
         assertTrue(added);
         
         // Make sure all nodes pick it up
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         
         // Add a durable and a non durable binding on node 1
         
         Queue queue6 = new MessagingQueue(1, "sub6", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue6.activate();
         
         added = office1.addBinding(new Binding(condition1, queue6, false), false);
         assertTrue(added);
         
         Queue queue7 = new MessagingQueue(1, "sub7", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue7.activate();
         
         added = office1.addBinding(new Binding(condition1, queue7, false), false);
         assertTrue(added);
         
         
         // Make sure all nodes pick them up
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(5, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         assertTrue(queues.contains(queue7));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(5, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         assertTrue(queues.contains(queue7));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(5, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         assertTrue(queues.contains(queue7));
               
         // Stop office 1
         office1.stop();
  
         // Need to sleep since it may take some time for the view changed request to reach the
         // members which causes the bindings to be removed.
         
         Thread.sleep(3000);
         
         // All it's bindings should be removed from the other nodes, including durable
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue3));
         assertTrue(queues.contains(queue4));
         assertTrue(queues.contains(queue5));
         
         // Stop office 2
         office2.stop();
         
         Thread.sleep(3000);
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         assertTrue(queues.contains(queue5));
         
         // Restart office 1 and office 2
         office1.start();
         
         office2.start();
                  
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue5));
         assertTrue(queues.contains(queue6));

         // Stop all offices
         
         office1.stop();
         office2.stop();
         office3.stop();
         
         // Start them all
         office1.start();
         office2.start();
         office3.start();
         
         // Only the durable queue should survive
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         assertTrue(queues.contains(queue6));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         assertTrue(queues.contains(queue6));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(1, queues.size());
         assertTrue(queues.contains(queue6));       
         
         //Unbind it
         
         log.info("Removing queue6 binding");
         removed = office1.removeBinding(queue6.getName(), false);
         assertNotNull(removed);         
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertTrue(queues.isEmpty());     
         
         
         //Bind another few more clustered
                           
         Queue queue8 = new MessagingQueue(1, "sub8", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue8.activate();
         
         Queue queue9 = new MessagingQueue(2, "sub9", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue9.activate();
         
         Queue queue10 = new MessagingQueue(2, "sub10", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue10.activate();
         
         //Bind on different conditions
         
         added = office1.addBinding(new Binding(condition1, queue8, false), false);
         assertTrue(added);
         
         added = office2.addBinding(new Binding(condition1, queue9, false), false);
         assertTrue(added);
         
         Condition condition2 = new SimpleCondition("condition2");
         
         added = office2.addBinding(new Binding(condition2, queue10, false), false);
         assertTrue(added);
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));
         
         //Now a couple of non clustered queues
         
         Queue queue11 = new MessagingQueue(1, "sub11", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue11.activate();
         
         Queue queue12 = new MessagingQueue(2, "sub12", channelIDManager.getID(), ms, pm, false, -1, null, false);
         queue12.activate();
         
         added = office1.addBinding(new Binding(condition1, queue11, false), false);
         assertTrue(added);
         
         added = office2.addBinding(new Binding(condition1, queue12, false), false);
         assertTrue(added);
         
         queues = office1.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));
         assertTrue(queues.contains(queue11));
         
         queues = office2.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(3, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));
         assertTrue(queues.contains(queue12));
         
         queues = office3.getQueuesForCondition(condition1, false);
         assertNotNull(queues);
         assertEquals(2, queues.size());
         assertTrue(queues.contains(queue8));
         assertTrue(queues.contains(queue9));        
         
         log.info("at end");    
         //Thread.sleep(10000000);
      }
      catch (Throwable e)
      {
         log.warn(e, e);
         throw e;
      }
      finally
      {
         if (office1 != null)
         {
         	try
         	{
         		office1.stop();
         	}
         	catch (Exception ignore)
         	{         		
         	}
         }
         
         if (office2 != null)
         {
         	try
         	{
         		office2.stop();
         	}
         	catch (Exception ignore)
         	{         		
         	}
         }
         
         if (office3 != null)
         {
         	try
         	{
         		office3.stop();
         	}
         	catch (Exception ignore)
         	{         		
         	}
         }
         
      }
   }
   
   public void testBindUnbindAll1() throws Throwable
   {
   	/*
      * 1.
      * a) queue is not known by cluster
      * b) bind all
      * c) verify all nodes get queue
      * d) unbind - verify unbound from all nodes
      * e) close down all nodes
      * f) start all nodes
      * g) verify queue is not known
      * */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);
         office2 = createClusteredPostOffice(2);
         office3 = createClusteredPostOffice(3);         
                             
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
          
         Collection bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         //Now unbind same node
         
         Binding removed = office1.removeBinding(queue1.getName(), true);
         assertNotNull(removed);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         //Bind again different node
         Queue queue2 = new MessagingQueue(2, "sub2", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue2.activate();
         
         added = office2.addBinding(new Binding(condition1, queue2, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue2.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue2.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue2.getName());
         
         //Close down all nodes
         
         office1.stop();
         
         dumpNodeIDView(office2);
         
         office2.stop();
         
         dumpNodeIDView(office3);
         
         office3.stop();        
         
         //Start all nodes
         
         office1.start();
         office2.start();
         office3.start();
         
         Thread.sleep(3000);
         
         //Verify the binding is there
         
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue2.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue2.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue2.getName());
         
         //Unbind different node
         
         removed = office3.removeBinding(queue2.getName(), true);
         assertNotNull(removed);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());                                        
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
      	}
      }
   }
   
   public void testBindUnbindAll2() throws Throwable
   {
      /* 
      * a) queue is known by cluster
      * b) bind all
      * c) verify nothing changes on cluster
      */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);
         office2 = createClusteredPostOffice(2);
         office3 = createClusteredPostOffice(3);         
                           
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
          
         Collection bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         //Bind again
         added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertFalse(added);
         
         Thread.sleep(3000);
          
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
                  
         //Now unbind same node
         
         Binding removed = office1.removeBinding(queue1.getName(), true);
         assertNotNull(removed);
         
         removed = office1.removeBinding(queue1.getName(), true);
         assertNull(removed);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());                                              
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
      	}
      }
   }
   
   public void testBindUnbindAll3() throws Throwable
   {
      /* a) start one node
      * b) queue is not known to cluster
      * c) bind all
      * d) start other nodes
      * d) verify other nodes pick it up
      */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);       
                              
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
          
         office2 = createClusteredPostOffice(2);       
         office3 = createClusteredPostOffice(3);       
                  
         Thread.sleep(3000);
         
         Collection bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
               
         //Unbind
         
         Binding removed = office1.removeBinding(queue1.getName(), true);
         assertNotNull(removed);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());                                    
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
      	}
      }
   }
   
   public void testBindUnbindAll4() throws Throwable
   {
      /* a) start one node
      * b) queue is not known to cluster
      * c) bind all
      * d) shutdown all nodes
      * e) startup all nodes
      * f) verify queue is on all nodes
      */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);       
                           
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
         
         office1.stop();
         
         //office1 = createClusteredPostOffice(1, "testgroup"); 
         office1.start();
         office2 = createClusteredPostOffice(2); 
         office3 = createClusteredPostOffice(3); 
          
         Thread.sleep(3000);
         
         Collection bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         //Now unbind same node
         
         Binding removed = office1.removeBinding(queue1.getName(), true);
         assertNotNull(removed);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());                  
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
      	}
      }
   }
   
   public void testBindUnbindAll5() throws Throwable
   {
   	/*
    * a) start one node
    * b) queue is not known
    * c) bind all
    * d) shutdown node
    * e) start other nodes
    * f) verify queue is not known
    * g) restart first node, verify queue is now known
      * */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);
                                   
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
         
         office1.stop();
         
         office2 = createClusteredPostOffice(2);
         office3 = createClusteredPostOffice(3);
         
         Collection bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());
             
         office1.start();
         
         Thread.sleep(3000);
            
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         //Now unbind same node                  
         
         Binding removed = office1.removeBinding(queue1.getName(), true);
         assertNotNull(removed);

         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());                       
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
      	}
      }
   }
   
   public void testBindUnbindAll6() throws Throwable
   {
   	/*
      * 1.
    * a) bind all non durable
    * b) make sure is picked up by all nodes
    * c) close down all nodes
    * d) restart them all - make sure is not there
    * e) bind again
    * f) make sure is picked up
    * g) take down one node
    * h) bring it back up
    * i) make sure it has quuee again
      * */
   	
      PostOffice office1 = null;
      PostOffice office2 = null;
      PostOffice office3 = null;
      
      try
      {                          
         office1 = createClusteredPostOffice(1);
         office2 = createClusteredPostOffice(2);
         office3 = createClusteredPostOffice(3);         
                           
         //Durable
         Queue queue1 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue1.activate();

         Condition condition1 = new SimpleCondition("condition1");
         
         //Add all binding
         boolean added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
          
         Collection bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         office1.stop();
         office2.stop();
         office3.stop();
      
         office1.start();
         office2.start();
         office3.start();
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office2.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         bindings = office3.getAllBindings();         
         assertTrue(bindings.isEmpty());
         
         added = office1.addBinding(new Binding(condition1, queue1, true), true);
         assertTrue(added);
         
         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());
         
         office3.stop();

         Thread.sleep(3000);
          
         bindings = office1.getAllBindings();         
         assertEquals(2, bindings.size());
         
         office3.start();

         Thread.sleep(3000);
         
         bindings = office1.getAllBindings();         
         assertGotAll(1, bindings, queue1.getName());
         
         bindings = office2.getAllBindings();         
         assertGotAll(2, bindings, queue1.getName());
         
         bindings = office3.getAllBindings();         
         assertGotAll(3, bindings, queue1.getName());                                                         
      }
      finally
      {
      	if (office1 != null)
      	{
      		try
      		{
      			office1.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office2 != null)
      	{
      		try
      		{
      			office2.stop();
      		}
      		catch (Exception ignore)
      		{         		
      		}
      	}

      	if (office3 != null)
      	{
      		try
      		{
      			office3.stop();
      		}         
      		catch (Exception ignore)
      		{         		
      		}
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

   public void testClusteredRouteWithFilterNonPersistent() throws Throwable
   {
      this.clusteredRouteWithFilter(false);
   }
   
   public void testClusteredRouteWithFilterPersistent() throws Throwable
   {
      this.clusteredRouteWithFilter(true);
   }
   
   public void testRouteSharedQueuePersistent() throws Throwable
   {
      this.routeSharedQueue(true);
   }
   
   public void testRouteSharedQueueNonPersistent() throws Throwable
   {
      this.routeSharedQueue(false);
   }
   
   public void testClusteredTransactionalRoutePersistent() throws Throwable
   {
   	this.clusteredTransactionalRoute(true);
   }
   
   public void testClusteredTransactionalRouteNonPersistent() throws Throwable
   {
   	this.clusteredTransactionalRoute(false);
   }
   
   public void testClusteredRouteFourNodesPersistent() throws Throwable
   {
   	this.clusteredRouteFourNodes(true);
   }
   
   public void testClusteredRouteFourNodesNonPersistent() throws Throwable
   {
   	this.clusteredRouteFourNodes(false);
   }
   
   public void testRouteWithNonClusteredQueuesNonPersistent() throws Throwable
   {
   	this.routeWithNonClusteredQueues(false);
   }
   
   public void testRouteWithNonClusteredQueuesPersistent() throws Throwable
   {
   	this.routeWithNonClusteredQueues(true);
   }
   
   public void testStartTxInternally() throws Throwable
   {
   	PostOffice office1 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);

   		Queue queue1 =  new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queue1.activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue1, false), false);
   		assertTrue(added);
   		
   		Queue queue2 =  new MessagingQueue(1, "queue2", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queue2.activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue2, false), false);
   		assertTrue(added);
   		
   		Queue queue3 =  new MessagingQueue(1, "queue3", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queue3.activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue3, false), false);
   		assertTrue(added);
   		
   		Queue queue4 =  new MessagingQueue(1, "queue4", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queue4.activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue4, false), false);
   		assertTrue(added);

   		SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue1.getLocalDistributor().add(receiver1);

   		SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue2.getLocalDistributor().add(receiver2);

   		SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue3.getLocalDistributor().add(receiver3);
   		
   		SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue4.getLocalDistributor().add(receiver4);

   		Message msg1 = CoreMessageFactory.createCoreMessage(1, true, null);      
   		MessageReference ref1 = ms.reference(msg1);  
   		boolean routed = office1.route(ref1, new SimpleCondition("condition1"), null);   
   		assertTrue(routed);

   		Message msg2 = CoreMessageFactory.createCoreMessage(2, true, null);      
   		MessageReference ref2 = ms.reference(msg2);         
   		routed = office1.route(ref2, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Message msg3 = CoreMessageFactory.createCoreMessage(3, true, null);      
   		MessageReference ref3 = ms.reference(msg3);         
   		routed = office1.route(ref3, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Thread.sleep(3000);
   		
   		List msgs = receiver1.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		Message msgRec1 = (Message)msgs.get(0);
   		assertTrue(msg1 == msgRec1);
   		Message msgRec2 = (Message)msgs.get(1);
   		assertTrue(msg2 == msgRec2);
   		Message msgRec3 = (Message)msgs.get(2);
   		assertTrue(msg3 == msgRec3);

   		receiver1.acknowledge(msgRec1, null);
   		receiver1.acknowledge(msgRec2, null);
   		receiver1.acknowledge(msgRec3, null);
   		msgs = queue1.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 
   		
   		msgs = receiver2.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertTrue(msg1 == msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertTrue(msg2 == msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertTrue(msg3 == msgRec3);

   		receiver2.acknowledge(msgRec1, null);
   		receiver2.acknowledge(msgRec2, null);
   		receiver2.acknowledge(msgRec3, null);
   		msgs = queue2.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 

   		msgs = receiver3.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertTrue(msg1 == msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertTrue(msg2 == msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertTrue(msg3 == msgRec3);

   		receiver3.acknowledge(msgRec1, null);
   		receiver3.acknowledge(msgRec2, null);
   		receiver3.acknowledge(msgRec3, null);
   		msgs = queue3.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 
   		
   		msgs = receiver4.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertTrue(msg1 == msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertTrue(msg2 == msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertTrue(msg3 == msgRec3);

   		receiver4.acknowledge(msgRec1, null);
   		receiver4.acknowledge(msgRec2, null);
   		receiver4.acknowledge(msgRec3, null);
   		msgs = queue4.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty());    		
   	}
   	finally
   	{
   		if (office1 != null)
   		{
            office1.removeBinding("queue1", true);
            office1.removeBinding("queue2", true);
            office1.removeBinding("queue3", true);
            office1.removeBinding("queue4", true);
            office1.stop();
   		}
   	}
   }
 
   public void testBindSameName() throws Throwable
   {
      PostOffice office1 = null;
      
      PostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice(1);
         
         office2 = createClusteredPostOffice(2);
         
         Queue queue1 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue1.activate();
         
         Condition condition1 = new SimpleCondition("queue1");
         
         boolean added = office1.addBinding(new Binding(condition1, queue1, false), false);
         assertTrue(added);

         Queue queue2 = new MessagingQueue(2, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue2.activate();

         added = office2.addBinding(new Binding(condition1, queue2, false), false);
         assertTrue(added);

         Queue queue3 = new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue3.activate();
         
         added = office1.addBinding(new Binding(condition1, queue3, false), false);         
         assertFalse(added);

         Queue queue4 =  new MessagingQueue(2, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queue4.activate();
         
         added = office2.addBinding(new Binding(condition1, queue4, false), false);
         assertFalse(added);
         
         Binding removed = office1.removeBinding("does not exist", false);
         assertNull(removed);
         
         removed = office1.removeBinding(queue1.getName(), false);
         assertNotNull(removed);

         removed = office2.removeBinding(queue2.getName(), false);                
         assertNotNull(removed);                  
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
   
 
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   private void clusteredTransactionalRoute(boolean persistent) throws Throwable
   {
      PostOffice office1 = null;
      
      PostOffice office2 = null;
      
      try
      {   
         //Start two offices
         
         office1 = createClusteredPostOffice(1);
         office2 = createClusteredPostOffice(2);
     
         Queue[] queues = new Queue[16];
         
         //condition1

         queues[0] = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[0].activate();
         boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[0], false), false);
         assertTrue(added);
         
         queues[1] = new MessagingQueue(1, "sub2", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[1].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[1], false), false);
         assertTrue(added);
         
         queues[2] = new MessagingQueue(2, "sub3", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[2].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[2], false), false);
         assertTrue(added);
         
         queues[3] = new MessagingQueue(2, "sub4", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[3].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[3], false), false);
         assertTrue(added);
         
         queues[4] = new MessagingQueue(2, "sub5", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[4].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[4], false), false);
         assertTrue(added);
         
         queues[5] = new MessagingQueue(1, "sub6", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[5].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[5], false), false);
         assertTrue(added);
         
         queues[6] = new MessagingQueue(1, "sub7", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[6].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[6], false), false);
         assertTrue(added);
         
         queues[7] = new MessagingQueue(1, "sub8", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[7].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[7], false), false);
         assertTrue(added);
         
         //condition2
         
         queues[8] = new MessagingQueue(1, "sub9", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[8].activate();
         added= office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[8], false), false);
         assertTrue(added);
         
         queues[9] = new MessagingQueue(1, "sub10", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[9].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[9], false), false);
         assertTrue(added);
         
         queues[10] = new MessagingQueue(2, "sub11", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[10].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[10], false), false);
         assertTrue(added);
         
         queues[11] = new MessagingQueue(2, "sub12", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[11].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[11], false), false);
         assertTrue(added);
         
         queues[12] = new MessagingQueue(2, "sub13", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[12].activate();
         added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[12], false), false);
         assertTrue(added);
         
         queues[13] = new MessagingQueue(1, "sub14", channelIDManager.getID(), ms, pm, false, -1, null, true);
         queues[13].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[13], false), false);
         assertTrue(added);
         
         queues[14] = new MessagingQueue(1, "sub15", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[14].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[14], false), false);
         assertTrue(added);
         
         queues[15] = new MessagingQueue(1, "sub16", channelIDManager.getID(), ms, pm, true, -1, null, true);
         queues[15].activate();
         added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[15], false), false);
         assertTrue(added);

         SimpleReceiver[] receivers = new SimpleReceiver[16];
         
         for (int i = 0; i < 16; i++)
         {
            receivers[i] = new SimpleReceiver("blah" + i, SimpleReceiver.ACCEPTING);
            queues[i].getLocalDistributor().add(receivers[i]);
         }
         
         //First for topic 1
         
         Message msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         MessageReference ref1 = ms.reference(msg1);
         
         Message msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         MessageReference ref2 = ms.reference(msg2);
         
         Transaction tx = tr.createTransaction();

         boolean routed = office1.route(ref1, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         routed = office1.route(ref2, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         
         for (int i = 0; i < 16; i++)
         {
         	log.info("i is " + i);
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         log.info("committing");
         tx.commit();
         log.info("committed");
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(3000);
         
         for (int i = 0; i < 16; i++)
         {
         	if (i >= 8 || (queues[i].getNodeID() == 2 && queues[i].isRecoverable()))
         	{     
         		//Shouldn't get message
         		List msgs = receivers[i].getMessages();
         		assertNotNull(msgs);
         		assertTrue(msgs.isEmpty());
         		msgs = queues[i].browse(null);
         		assertNotNull(msgs);
         		assertTrue(msgs.isEmpty());
         	}
         	else
         	{
         		//Should get message
         		log.info("is is " + i);
         		log.info("trying with receiver " + receivers[i]);
         		List msgs = receivers[i].getMessages();
         		assertNotNull(msgs);
         		assertEquals(2, msgs.size());
         		Message msgRec1 = (Message)msgs.get(0);
         		assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
         		Message msgRec2 = (Message)msgs.get(1);
         		assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
         		receivers[i].acknowledge(msgRec1, null);
         		receivers[i].acknowledge(msgRec2, null);
         		msgs = queues[i].browse(null);
         		assertNotNull(msgs);            
         		assertTrue(msgs.isEmpty());                        
         		receivers[i].clear();
         	}
         }
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         routed = office1.route(ref2, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(3000);         
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         
         // Now for topic 2
         
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);    
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);     
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office2.route(ref1, new SimpleCondition("condition2"), tx);         
         assertTrue(routed);
         routed = office2.route(ref2, new SimpleCondition("condition2"), tx);         
         assertTrue(routed);
                           
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.commit();
         
         //Messages are sent asych so may take some finite time to arrive
         Thread.sleep(3000);
         
         for (int i = 8; i < 16; i++)
         { 
	         if (i < 8 || (queues[i].getNodeID() == 1 && queues[i].isRecoverable()))
	      	{     
	         	//	Shouldn't get message
	      		List msgs = receivers[i].getMessages();
	      		assertNotNull(msgs);
	      		assertTrue(msgs.isEmpty());
	      		msgs = queues[i].browse(null);
	      		assertNotNull(msgs);
	      		assertTrue(msgs.isEmpty());
	      	}
	      	else
	      	{
	      		// Should get message
	      		log.info("is is " + i);
	      		log.info("trying with receiver " + receivers[i]);
	      		List msgs = receivers[i].getMessages();
	      		assertNotNull(msgs);
	      		assertEquals(2, msgs.size());
	      		Message msgRec1 = (Message)msgs.get(0);
	      		assertEquals(msg1.getMessageID(), msgRec1.getMessageID());
	      		Message msgRec2 = (Message)msgs.get(1);
	      		assertEquals(msg2.getMessageID(), msgRec2.getMessageID());            
	      		receivers[i].acknowledge(msgRec1, null);
	      		receivers[i].acknowledge(msgRec2, null);
	      		msgs = queues[i].browse(null);
	      		assertNotNull(msgs);            
	      		assertTrue(msgs.isEmpty());                        
	      		receivers[i].clear();
	      	}
         }
               
         msg1 = CoreMessageFactory.createCoreMessage(1, persistent, null);;      
         ref1 = ms.reference(msg1);
         
         msg2 = CoreMessageFactory.createCoreMessage(2, persistent, null);;      
         ref2 = ms.reference(msg2);
         
         tx = tr.createTransaction();

         routed = office1.route(ref1, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         routed = office1.route(ref2, new SimpleCondition("condition1"), tx);         
         assertTrue(routed);
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }
         
         tx.rollback();
         
         for (int i = 0; i < 16; i++)
         {
            List msgs = receivers[i].getMessages();
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
            msgs = queues[i].browse(null);
            assertNotNull(msgs);
            assertTrue(msgs.isEmpty());
         }         
      }
      finally
      {
         if (office1 != null)
         {
            try
            {
               office1.removeBinding("sub7", false);
               office1.removeBinding("sub8", false);           
               office1.removeBinding("sub15", false);
               office1.removeBinding("sub16", false);
            }
            catch (Exception ignore)
            {
               ignore.printStackTrace();
            }
                        
            office1.stop();
         }
         
         if (office2 != null)
         {
            try
            {
               office2.removeBinding("sub5", false);
               office2.removeBinding("sub13", false);
            }
            catch (Exception ignore)
            {
               ignore.printStackTrace();
            }
            
            office2.stop();
         }
      }
   }
   
   private void clusteredRouteWithFilter(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);

   		SimpleFilter filter1 = new SimpleFilter(2);
   		SimpleFilter filter2 = new SimpleFilter(3);

   		Queue queue1 =  new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, filter1, true);
   		queue1.activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue1, false), false);
   		assertTrue(added);

   		Queue queue2 = new MessagingQueue(2, "queue2", channelIDManager.getID(), ms, pm, false, -1, filter2, true);
   		queue2.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queue2, false), false);
   		assertTrue(added);

   		Queue queue3 = new MessagingQueue(2, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue3.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queue3, false), false);
   		assertTrue(added);

   		SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue1.getLocalDistributor().add(receiver1);

   		SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue2.getLocalDistributor().add(receiver2);

   		SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue3.getLocalDistributor().add(receiver3);

   		Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref1 = ms.reference(msg1);  
   		boolean routed = office1.route(ref1, new SimpleCondition("condition1"), null);   
   		assertTrue(routed);


   		Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
   		MessageReference ref2 = ms.reference(msg2);         
   		routed = office1.route(ref2, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Message msg3 = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);      
   		MessageReference ref3 = ms.reference(msg3);         
   		routed = office1.route(ref3, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Thread.sleep(3000);

   		List msgs = receiver1.getMessages();
   		assertNotNull(msgs);
   		assertEquals(1, msgs.size());
   		Message msgRec = (Message)msgs.get(0);
   		assertEquals(msg2, msgRec);
   		receiver1.acknowledge(msgRec, null);
   		msgs = queue1.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty());  

   		msgs = receiver2.getMessages();
   		assertNotNull(msgs);
   		assertEquals(1, msgs.size());
   		msgRec = (Message)msgs.get(0);
   		assertEquals(msg3, msgRec);
   		receiver2.acknowledge(msgRec, null);
   		msgs = queue2.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty());  

   		msgs = receiver3.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		Message msgRec1 = (Message)msgs.get(0);
   		assertEquals(msg1, msgRec1);
   		Message msgRec2 = (Message)msgs.get(1);
   		assertEquals(msg2, msgRec2);
   		Message msgRec3 = (Message)msgs.get(2);
   		assertEquals(msg3, msgRec3);

   		receiver3.acknowledge(msgRec1, null);
   		receiver3.acknowledge(msgRec2, null);
   		receiver3.acknowledge(msgRec3, null);
   		msgs = queue3.browse(null);
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
   
   private void clusteredRouteFourNodes(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;
   	
   	PostOffice office3 = null;

   	PostOffice office4 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);
   		office3 = createClusteredPostOffice(3);
   		office4 = createClusteredPostOffice(4);

   		Queue queue1 =  new MessagingQueue(1, "queue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue1.activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue1, false), false);
   		assertTrue(added);
   		
   		Queue queue2 =  new MessagingQueue(2, "queue2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue2.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queue2, false), false);
   		assertTrue(added);
   		
   		Queue queue3 =  new MessagingQueue(3, "queue3", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue3.activate();
   		added = office3.addBinding(new Binding(new SimpleCondition("condition1"), queue3, false), false);
   		assertTrue(added);
   		
   		Queue queue4 =  new MessagingQueue(4, "queue4", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue4.activate();
   		added = office4.addBinding(new Binding(new SimpleCondition("condition1"), queue4, false), false);
   		assertTrue(added);

   		SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue1.getLocalDistributor().add(receiver1);

   		SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue2.getLocalDistributor().add(receiver2);

   		SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue3.getLocalDistributor().add(receiver3);
   		
   		SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue4.getLocalDistributor().add(receiver4);

   		Message msg1 = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref1 = ms.reference(msg1);  
   		boolean routed = office1.route(ref1, new SimpleCondition("condition1"), null);   
   		assertTrue(routed);

   		Message msg2 = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
   		MessageReference ref2 = ms.reference(msg2);         
   		routed = office1.route(ref2, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Message msg3 = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);      
   		MessageReference ref3 = ms.reference(msg3);         
   		routed = office1.route(ref3, new SimpleCondition("condition1"), null);      
   		assertTrue(routed);

   		Thread.sleep(3000);
   		
   		List msgs = receiver1.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		Message msgRec1 = (Message)msgs.get(0);
   		assertEquals(msg1,  msgRec1);
   		Message msgRec2 = (Message)msgs.get(1);
   		assertEquals(msg2, msgRec2);
   		Message msgRec3 = (Message)msgs.get(2);
   		assertEquals(msg3, msgRec3);

   		receiver1.acknowledge(msgRec1, null);
   		receiver1.acknowledge(msgRec2, null);
   		receiver1.acknowledge(msgRec3, null);
   		msgs = queue1.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 
   		
   		msgs = receiver2.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertEquals(msg1, msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertEquals(msg2, msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertEquals(msg3, msgRec3);

   		receiver2.acknowledge(msgRec1, null);
   		receiver2.acknowledge(msgRec2, null);
   		receiver2.acknowledge(msgRec3, null);
   		msgs = queue2.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 

   		msgs = receiver3.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertEquals(msg1, msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertEquals(msg2, msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertEquals(msg3, msgRec3);

   		receiver3.acknowledge(msgRec1, null);
   		receiver3.acknowledge(msgRec2, null);
   		receiver3.acknowledge(msgRec3, null);
   		msgs = queue3.browse(null);
   		assertNotNull(msgs);
   		assertTrue(msgs.isEmpty()); 
   		
   		msgs = receiver4.getMessages();
   		assertNotNull(msgs);
   		assertEquals(3, msgs.size());
   		msgRec1 = (Message)msgs.get(0);
   		assertEquals(msg1, msgRec1);
   		msgRec2 = (Message)msgs.get(1);
   		assertEquals(msg2, msgRec2);
   		msgRec3 = (Message)msgs.get(2);
   		assertEquals(msg3, msgRec3);

   		receiver4.acknowledge(msgRec1, null);
   		receiver4.acknowledge(msgRec2, null);
   		receiver4.acknowledge(msgRec3, null);
   		msgs = queue4.browse(null);
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
   		
   		if (office3 != null)
   		{
   			office3.stop();
   		}

   		if (office4 != null)
   		{
   			office4.stop();
   		}

   	}
   }
   
   

   private void clusteredRoute(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);

   		//A mixture of durable and non durable queues

   		Queue[] queues = new Queue[16];

   		//condition1

   		queues[0] = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[0].activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[0], false), false);
   		assertTrue(added);

   		queues[1] = new MessagingQueue(1, "sub2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[1].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[1], false), false);
   		assertTrue(added);

   		queues[2] = new MessagingQueue(2, "sub3", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[2].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[2], false), false);
   		assertTrue(added);

   		queues[3] = new MessagingQueue(2, "sub4", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[3].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[3], false), false);
   		assertTrue(added);

   		//durable

   		queues[4] = new MessagingQueue(2, "sub5", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[4].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[4], false), false);
   		assertTrue(added);

   		queues[5] = new MessagingQueue(1, "sub6", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[5].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[5], false), false);
   		assertTrue(added);

   		//durable

   		queues[6] = new MessagingQueue(1, "sub7", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[6].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[6], false), false);
   		assertTrue(added);

   		//durable

   		queues[7] = new MessagingQueue(1, "sub8", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[7].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[7], false), false);
   		assertTrue(added);

   		//condition2


   		queues[8] = new MessagingQueue(1, "sub9", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[8].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[8], false), false);
   		assertTrue(added);

   		queues[9] = new MessagingQueue(1, "sub10", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[9].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[9], false), false);
   		assertTrue(added);

   		queues[10] = new MessagingQueue(2, "sub11", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[10].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[10], false), false);
   		assertTrue(added);

   		queues[11] = new MessagingQueue(2, "sub12", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[11].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[11], false), false);
   		assertTrue(added);

   		//durable

   		queues[12] = new MessagingQueue(2, "sub13", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[12].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[12], false), false);
   		assertTrue(added);

   		queues[13] = new MessagingQueue(1, "sub14", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[13].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[13], false), false);
   		assertTrue(added);

   		//durable

   		queues[14] = new MessagingQueue(1, "sub15", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[14].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[14], false), false);
   		assertTrue(added);

   		queues[15] = new MessagingQueue(1, "sub16", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[15].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[15], false), false);
   		assertTrue(added);

   		SimpleReceiver[] receivers = new SimpleReceiver[16];

   		for (int i = 0; i < 16; i++)
   		{
   			receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   			queues[i].getLocalDistributor().add(receivers[i]);
   		}

   		Message msg = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref = ms.reference(msg);         

   		boolean routed = office1.route(ref, new SimpleCondition("condition1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Durable queues on remote node should never get the message

   		for (int i = 0; i < 16; i++)
   		{
   			if (i >= 8 || (queues[i].getNodeID() == 2 && queues[i].isRecoverable()))
   			{
   				this.checkNotGetsMessage(queues[i], receivers[i]);
   			}
   			else
   			{
   				//Should get the message
   				this.checkGetsMessage(queues[i], receivers[i], msg);
   			}

   		}

   		//Now route to condition2

   		msg = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("condition2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		for (int i = 0; i < 16; i++)
   		{
   			if (i < 8 || (queues[i].getNodeID() == 1 && queues[i].isRecoverable()))
   			{
   				//Shouldn't get the message
   				this.checkNotGetsMessage(queues[i], receivers[i]);
   			}
   			else
   			{
   				//Should get the message
   				this.checkGetsMessage(queues[i], receivers[i], msg);
   			}

   		}
   	}
   	finally
   	{
   		if (office1 != null)
   		{
   			try
   			{              
   				office1.removeBinding("sub7", false);
   				office1.removeBinding("sub8", false);               
   				office1.removeBinding("sub15", false);
   				office1.removeBinding("sub16", false);
   			}
   			catch (Exception ignore)
   			{
   				ignore.printStackTrace();
   			}

   			office1.stop();
   		}

   		if (office2 != null)
   		{
   			try
   			{
   				office2.removeBinding("sub5", false);
   				office2.removeBinding("sub13", false);
   			}
   			catch (Exception ignore)
   			{     
   				ignore.printStackTrace();
   			}
   			office2.stop();
   		}

   	}
   }
   
   
   private void routeWithNonClusteredQueues(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);

   		Queue[] queues = new Queue[16];

   		//condition1

   		queues[0] = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, false);
   		queues[0].activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[0], false), false);
   		assertTrue(added);

   		queues[1] = new MessagingQueue(1, "sub2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[1].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[1], false), false);
   		assertTrue(added);

   		queues[2] = new MessagingQueue(2, "sub3", channelIDManager.getID(), ms, pm, false, -1, null, false);
   		queues[2].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[2], false), false);
   		assertTrue(added);

   		queues[3] = new MessagingQueue(2, "sub4", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[3].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[3], false), false);
   		assertTrue(added);

   		queues[4] = new MessagingQueue(2, "sub5", channelIDManager.getID(), ms, pm, true, -1, null, false);
   		queues[4].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queues[4], false), false);
   		assertTrue(added);

   		queues[5] = new MessagingQueue(1, "sub6", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[5].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[5], false), false);
   		assertTrue(added);

   		queues[6] = new MessagingQueue(1, "sub7", channelIDManager.getID(), ms, pm, true, -1, null, false);
   		queues[6].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[6], false), false);
   		assertTrue(added);

   		queues[7] = new MessagingQueue(1, "sub8", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[7].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queues[7], false), false);
   		assertTrue(added);

   		//condition2


   		queues[8] = new MessagingQueue(1, "sub9", channelIDManager.getID(), ms, pm, false, -1, null, false);
   		queues[8].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[8], false), false);
   		assertTrue(added);

   		queues[9] = new MessagingQueue(1, "sub10", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[9].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[9], false), false);
   		assertTrue(added);

   		queues[10] = new MessagingQueue(2, "sub11", channelIDManager.getID(), ms, pm, false, -1, null, false);
   		queues[10].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[10], false), false);
   		assertTrue(added);

   		queues[11] = new MessagingQueue(2, "sub12", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[11].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[11], false), false);
   		assertTrue(added);

   		queues[12] = new MessagingQueue(2, "sub13", channelIDManager.getID(), ms, pm, true, -1, null, false);
   		queues[12].activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queues[12], false), false);
   		assertTrue(added);

   		queues[13] = new MessagingQueue(1, "sub14", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queues[13].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[13], false), false);
   		assertTrue(added);

   		queues[14] = new MessagingQueue(1, "sub15", channelIDManager.getID(), ms, pm, true, -1, null, false);
   		queues[14].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[14], false), false);
   		assertTrue(added);

   		queues[15] = new MessagingQueue(1, "sub16", channelIDManager.getID(), ms, pm, true, -1, null, true);
   		queues[15].activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queues[15], false), false);
   		assertTrue(added);

   		SimpleReceiver[] receivers = new SimpleReceiver[16];

   		for (int i = 0; i < 16; i++)
   		{
   			receivers[i] = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   			queues[i].getLocalDistributor().add(receivers[i]);
   		}

   		Message msg = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref = ms.reference(msg);         

   		boolean routed = office1.route(ref, new SimpleCondition("condition1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Durable queues on remote node should never get the message - neither should non clustered ones

   		for (int i = 0; i < 16; i++)
   		{
   			if (i >= 8 || (queues[i].getNodeID() == 2 && queues[i].isRecoverable())
   					|| (queues[i].getNodeID() == 2 && !queues[i].isClustered()))
   			{
   				this.checkNotGetsMessage(queues[i], receivers[i]);
   			}
   			else
   			{
   				//Should get the message
   				this.checkGetsMessage(queues[i], receivers[i], msg);
   			}
   		}

   		//Now route to condition2

   		msg = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("condition2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		for (int i = 0; i < 16; i++)
   		{
   			if (i < 8 || (queues[i].getNodeID() == 1 && queues[i].isRecoverable())
   					|| (queues[i].getNodeID() == 1 && !queues[i].isClustered()))
   			{
   				//Shouldn't get the message
   				this.checkNotGetsMessage(queues[i], receivers[i]);
   			}
   			else
   			{
   				//Should get the message
   				this.checkGetsMessage(queues[i], receivers[i], msg);
   			}

   		}
   	}
   	finally
   	{
   		if (office1 != null)
   		{
   			try
   			{              
   				office1.removeBinding("sub7", false);
   				office1.removeBinding("sub8", false);               
   				office1.removeBinding("sub15", false);
   				office1.removeBinding("sub16", false);
   			}
   			catch (Exception ignore)
   			{
   				ignore.printStackTrace();
   			}

   			office1.stop();
   		}

   		if (office2 != null)
   		{
   			try
   			{
   				office2.removeBinding("sub5", false);
   				office2.removeBinding("sub13", false);
   			}
   			catch (Exception ignore)
   			{     
   				ignore.printStackTrace();
   			}
   			office2.stop();
   		}

   	}
   }



   /*
    * Queues with same name on different nodes of the cluster.
    * If queue is routed to locally it shouldn't be routed to on other nodes
    * 
    */
   private void routeSharedQueue(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);

   		//queue1

   		Queue queue0 = new MessagingQueue(1, "myqueue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue0.activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("myqueue1"), queue0, false), false);
   		assertTrue(added);

   		Queue queue1 = new MessagingQueue(2, "myqueue1", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue1.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("myqueue1"), queue1, false), false);
   		assertTrue(added);


   		//queue2

   		Queue queue2 = new MessagingQueue(1, "myqueue2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue2.activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("myqueue2"), queue2, false), false);
   		assertTrue(added);

   		Queue queue3 = new MessagingQueue(2, "myqueue2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue3.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("myqueue2"), queue3, false), false);
   		assertTrue(added);


   		SimpleReceiver receiver0 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue0.getLocalDistributor().add(receiver0);

   		SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue1.getLocalDistributor().add(receiver1);

   		SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue2.getLocalDistributor().add(receiver2);

   		SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue3.getLocalDistributor().add(receiver3);

   		//Route to myqueue1 from office1         

   		Message msg = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref = ms.reference(msg);         

   		boolean routed = office1.route(ref, new SimpleCondition("myqueue1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Only queue0 should get the message
   		checkGetsMessage(queue0, receiver0, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue2, receiver2);

   		checkNotGetsMessage(queue3, receiver3);

   		//	Route to myqueue1 from office 2      

   		msg = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("myqueue1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Only queue1 should get the message
   		checkGetsMessage(queue1, receiver1, msg);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue2, receiver2);

   		checkNotGetsMessage(queue3, receiver3);


   		//Now route to condition2 from office 1

   		msg = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office1.route(ref, new SimpleCondition("myqueue2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		// Only queue2 should get the message
   		checkGetsMessage(queue2, receiver2, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue3, receiver3);


   		//Now route to condition2 from office 2

   		msg = CoreMessageFactory.createCoreMessage(4, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("myqueue2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		// Only queue3 should get the message
   		checkGetsMessage(queue3, receiver3, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue2, receiver2);
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


   private void routeWithFilter(boolean persistentMessage) throws Throwable
   {
   	PostOffice office1 = null;

   	PostOffice office2 = null;

   	try
   	{   
   		office1 = createClusteredPostOffice(1);
   		office2 = createClusteredPostOffice(2);


   		Queue queue0 = new MessagingQueue(1, "sub1", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue0.activate();
   		boolean added = office1.addBinding(new Binding(new SimpleCondition("condition1"), queue0, false), false);
   		assertTrue(added);

   		Queue queue1 = new MessagingQueue(2, "sub2", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue1.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition1"), queue1, false), false);
   		assertTrue(added);


   		Queue queue2 = new MessagingQueue(1, "sub3", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue2.activate();
   		added = office1.addBinding(new Binding(new SimpleCondition("condition2"), queue2, false), false);
   		assertTrue(added);

   		Queue queue3 = new MessagingQueue(2, "sub4", channelIDManager.getID(), ms, pm, false, -1, null, true);
   		queue3.activate();
   		added = office2.addBinding(new Binding(new SimpleCondition("condition2"), queue3, false), false);
   		assertTrue(added);


   		SimpleReceiver receiver0 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue0.getLocalDistributor().add(receiver0);

   		SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue1.getLocalDistributor().add(receiver1);

   		SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue2.getLocalDistributor().add(receiver2);

   		SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
   		queue3.getLocalDistributor().add(receiver3);

   		//Route to condition1 from office1         

   		Message msg = CoreMessageFactory.createCoreMessage(1, persistentMessage, null);      
   		MessageReference ref = ms.reference(msg);         

   		boolean routed = office1.route(ref, new SimpleCondition("myqueue1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Only queue0 should get the message
   		checkGetsMessage(queue0, receiver0, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue2, receiver2);

   		checkNotGetsMessage(queue3, receiver3);

   		//	Route to myqueue1 from office 2      

   		msg = CoreMessageFactory.createCoreMessage(2, persistentMessage, null);      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("myqueue1"), null);         
   		assertTrue(routed);

   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		//Only queue1 should get the message
   		checkGetsMessage(queue1, receiver1, msg);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue2, receiver2);

   		checkNotGetsMessage(queue3, receiver3);


   		//Now route to condition2 from office 1

   		msg = CoreMessageFactory.createCoreMessage(3, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office1.route(ref, new SimpleCondition("myqueue2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		// Only queue2 should get the message
   		checkGetsMessage(queue2, receiver2, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue3, receiver3);


   		//Now route to condition2 from office 2

   		msg = CoreMessageFactory.createCoreMessage(4, persistentMessage, null);;      
   		ref = ms.reference(msg);         

   		routed = office2.route(ref, new SimpleCondition("myqueue2"), null);         
   		assertTrue(routed);
   		//Messages are sent asych so may take some finite time to arrive
   		Thread.sleep(3000);

   		// Only queue3 should get the message
   		checkGetsMessage(queue3, receiver3, msg);

   		checkNotGetsMessage(queue1, receiver1);

   		checkNotGetsMessage(queue0, receiver0);

   		checkNotGetsMessage(queue2, receiver2);
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

   private void dumpNodeIDView(PostOffice postOffice)
   {
   	Set view = postOffice.nodeIDView();

   	log.info("=== node id view ==");

   	Iterator iter = view.iterator();

   	while (iter.hasNext())
   	{
   		log.info("Node:" + iter.next());
   	}

   	log.info("==================");
   }

   private void assertGotAll(int nodeId, Collection bindings, String queueName)
   {

   	log.info("============= dumping bindings ========");

   	Iterator iter = bindings.iterator();

   	while (iter.hasNext())
   	{
   		Binding binding = (Binding)iter.next();

   		log.info("Binding: " + binding);
   	}

   	log.info("========= end dump==========");

   	assertEquals(3, bindings.size());

   	iter = bindings.iterator();

   	boolean got1 = false;
   	boolean got2 = false;
   	boolean got3 = false;         
   	while (iter.hasNext())
   	{
   		Binding binding = (Binding)iter.next();

   		log.info("binding node id " + binding.queue.getNodeID());

   		assertEquals(queueName, binding.queue.getName());
   		if (binding.queue.getNodeID() == nodeId)
   		{
   			assertTrue(binding.allNodes);
   		}
   		else
   		{
   			assertFalse(binding.allNodes);
   		}

   		if (binding.queue.getNodeID() == 1)
   		{
   			got1 = true;
   		}
   		if (binding.queue.getNodeID() == 2)
   		{
   			got2 = true;
   		}
   		if (binding.queue.getNodeID() == 3)
   		{
   			got3 = true;
   		}    	
   	}         
   	assertTrue(got1 && got2 && got3);
   }


   private void checkGetsMessage(Queue queue, SimpleReceiver receiver, Message msg) throws Throwable
   {
   	List msgs = receiver.getMessages();
   	assertNotNull(msgs);
   	assertEquals(1, msgs.size());
   	Message msgRec = (Message)msgs.get(0);
   	assertEquals(msg.getMessageID(), msgRec.getMessageID());
   	receiver.acknowledge(msgRec, null);
   	msgs = queue.browse(null);
   	assertNotNull(msgs);
   	assertTrue(msgs.isEmpty()); 
   	receiver.clear();
   }

   private void checkNotGetsMessage(Queue queue, SimpleReceiver receiver) throws Throwable
   {
   	List msgs = receiver.getMessages();
   	assertNotNull(msgs);
   	assertTrue(msgs.isEmpty());
   	msgs = queue.browse(null);
   	assertNotNull(msgs);
   	assertTrue(msgs.isEmpty());
   }
   
   // Inner classes --------------------------------------------------------------------------------

}



