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

import java.util.List;

import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusteredPostOfficeImpl;
import org.jboss.messaging.core.plugin.postoffice.cluster.FavourLocalRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.RedistributionPolicy;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.plugin.postoffice.DefaultPostOfficeTest;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

public class FavourLocalRouterTest extends DefaultPostOfficeTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public FavourLocalRouterTest(String name)
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
   
   public void testNoLocalQueue() throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
          
      try
      {   
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         office1 = createClusteredPostOffice("node1", "testgroup");
         
         office2 = createClusteredPostOffice("node2", "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue("node1", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding1 = office1.bindClusteredQueue("queue1", queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue("node2", "queue1", im.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null);
         
         Binding binding2 = office2.bindClusteredQueue("queue1", queue1);
      
         final int NUM_MESSAGES = 10;
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message msg = CoreMessageFactory.createCoreMessage(i, false, null);      
            MessageReference ref = ms.reference(msg);         
            boolean routed = office1.route(ref, "queue1", null);
         }
         
         //We have a favour local routing policy so all messages should be in queue1
         List msgs = queue1.browse();
         assertEquals(NUM_MESSAGES, msgs.size());
         
         msgs = queue2.browse();
         assertEquals(0, msgs.size());
         
         office1.unbindClusteredQueue("queue1");
         
         //Send some more messages
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message msg = CoreMessageFactory.createCoreMessage(i + 10, false, null);      
            MessageReference ref = ms.reference(msg);         
            boolean routed = office1.route(ref, "queue1", null);
         }
         
         //There is no queue1 on node1 any more so the messages should be on node2
         
         msgs = queue2.browse();
         assertEquals(NUM_MESSAGES, msgs.size());
                           
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
   
   
   
   protected ClusteredPostOffice createClusteredPostOffice(String nodeId, String groupName) throws Exception
   {
      RedistributionPolicy redistPolicy = new NullRedistributionPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new FavourLocalRouterFactory();
      
      ClusteredPostOfficeImpl postOffice = 
         new ClusteredPostOfficeImpl(sc.getDataSource(), sc.getTransactionManager(),
                                 null, true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 5000, 5000, redistPolicy, 1000, rf);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   

}



