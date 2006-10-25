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
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.NullMessagePullPolicy;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.PostOfficeTestBase;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A DefaultClusteredPostOfficeWithDefaultRouterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultClusteredPostOfficeWithDefaultRouterTest extends PostOfficeTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public DefaultClusteredPostOfficeWithDefaultRouterTest(String name)
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
   
   public void testNotLocalPersistent() throws Throwable
   {
      notLocal(true);
   }
   
   public void testNotLocalNonPersistent() throws Throwable
   {
      notLocal(false);
   }
   
   public void testLocalPersistent() throws Throwable
   {
      local(true);
   }
   
   public void testLocalNonPersistent() throws Throwable
   {
      local(false);
   }
   
   protected void notLocal(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
      
      ClusteredPostOffice office6 = null;
          
      try
      {   
         office1 = createClusteredPostOffice(1, "testgroup");
         
         office2 = createClusteredPostOffice(2, "testgroup");
         
         office3 = createClusteredPostOffice(3, "testgroup");
         
         office4 = createClusteredPostOffice(4, "testgroup");
         
         office5 = createClusteredPostOffice(5, "testgroup");
         
         office6 = createClusteredPostOffice(6, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office2, 2, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding1 = office2.bindClusteredQueue("topic", queue1);
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office3, 3, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding2 = office3.bindClusteredQueue("topic", queue2);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office4, 4, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding3 = office4.bindClusteredQueue("topic", queue3);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office5, 5, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding4 = office5.bindClusteredQueue("topic", queue4);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office6, 6, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding5 = office6.bindClusteredQueue("topic", queue5);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
               
         List msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkContainsAndAcknowledge(msgs, receiver3, queue1);                           
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkContainsAndAcknowledge(msgs, receiver4, queue1);                                    
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkContainsAndAcknowledge(msgs, receiver5, queue1); 
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office1, 1, null);         
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
                     
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
         
         if (office5 != null)
         {            
            office5.stop();
         }
         
         if (office6 != null)
         {
            office6.stop();
         }
      }
   }
   
   
   
   
   protected void local(boolean persistent) throws Throwable
   {
      ClusteredPostOffice office1 = null;
      
      ClusteredPostOffice office2 = null;
      
      ClusteredPostOffice office3 = null;
      
      ClusteredPostOffice office4 = null;
      
      ClusteredPostOffice office5 = null;
      
      ClusteredPostOffice office6 = null;
          
      try
      {   
         office1 = createClusteredPostOffice(1, "testgroup");
         
         office2 = createClusteredPostOffice(2, "testgroup");
         
         office3 = createClusteredPostOffice(3, "testgroup");
         
         office4 = createClusteredPostOffice(4, "testgroup");
         
         office5 = createClusteredPostOffice(5, "testgroup");
         
         office6 = createClusteredPostOffice(6, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office2, 2, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding1 = office2.bindClusteredQueue("topic", queue1);
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office3, 3, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding2 = office3.bindClusteredQueue("topic", queue2);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         LocalClusteredQueue queue3 = new LocalClusteredQueue(office4, 4, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding3 = office4.bindClusteredQueue("topic", queue3);
         SimpleReceiver receiver3 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue3.add(receiver3);
         
         LocalClusteredQueue queue4 = new LocalClusteredQueue(office5, 5, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding4 = office5.bindClusteredQueue("topic", queue4);
         SimpleReceiver receiver4 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue4.add(receiver4);
         
         LocalClusteredQueue queue5 = new LocalClusteredQueue(office6, 6, "queue1", channelIdManager.getId(), ms, pm, true, false, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding5 = office6.bindClusteredQueue("topic", queue5);
         SimpleReceiver receiver5 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue5.add(receiver5);
               
         List msgs = sendMessages("topic", persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office2, 3, null);         
         checkContainsAndAcknowledge(msgs, receiver1, queue1);         
         checkEmpty(receiver2);
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         
         msgs = sendMessages("topic", persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
         msgs = sendMessages("topic", persistent, office3, 3, null); 
         checkEmpty(receiver1);
         checkContainsAndAcknowledge(msgs, receiver2, queue1);                  
         checkEmpty(receiver3);
         checkEmpty(receiver4);
         checkEmpty(receiver5);
         
                     
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
         
         if (office5 != null)
         {            
            office5.stop();
         }
         
         if (office6 != null)
         {
            office6.stop();
         }
      }
   }
   
   
   
   protected ClusteredPostOffice createClusteredPostOffice(int nodeId, String groupName) throws Exception
   {
      MessagePullPolicy redistPolicy = new NullMessagePullPolicy();
      
      FilterFactory ff = new SimpleFilterFactory();
      
      ClusterRouterFactory rf = new DefaultRouterFactory();
      
      DefaultClusteredPostOffice postOffice = 
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 sc.getClusteredPostOfficeSQLProperties(), true, nodeId, "Clustered", ms, pm, tr, ff, pool,
                                 groupName,
                                 JGroupsUtil.getControlStackProperties(),
                                 JGroupsUtil.getDataStackProperties(),
                                 5000, 5000, redistPolicy, rf, 1000);
      
      postOffice.start();      
      
      return postOffice;
   }

   // Private -------------------------------------------------------
   
   
   // Inner classes -------------------------------------------------
   

}



