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
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.NullMessagePullPolicy;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.base.ClusteringTestBase;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A RecoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class RecoveryTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public RecoveryTest(String name)
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
   
   public void testCrashBeforePersist() throws Exception
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding1 =
            office1.bindClusteredQueue("topic1", queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue2", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding2 =
            office2.bindClusteredQueue("topic1", queue2);
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         //This will make it fail after casting but before persisting the message in the db
         office1.setFail(true, false);
         
         Transaction tx = tr.createTransaction();
         
         final int NUM_MESSAGES = 10;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message msg = CoreMessageFactory.createCoreMessage(i);   
            msg.setReliable(true);
            
            MessageReference ref = ms.reference(msg);  
            
            office1.route(ref, "topic1", tx);
         }
         
         Thread.sleep(1000);
         
         List msgs = receiver1.getMessages();
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertTrue(msgs.isEmpty());
         
         try
         {
            //An exception should be thrown            
            tx.commit();
            fail();                        
         }
         catch (TransactionException e)
         {
            //Ok
         }
         
         Thread.sleep(1000);
         
         msgs = receiver1.getMessages();
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertTrue(msgs.isEmpty());
         
         //We now kill the office - this should make the other office do it's transaction check
         office1.stop();
         
         Thread.sleep(1000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         //The tx should be removed from the holding area and nothing should be received
         //remember node1 has now crashed so no point checking receiver1
         
         msgs = receiver2.getMessages();
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
   
   public void testCrashAfterPersist() throws Exception
   {
      DefaultClusteredPostOffice office1 = null;
      
      DefaultClusteredPostOffice office2 = null;
      
      try
      {      
         office1 = (DefaultClusteredPostOffice)createClusteredPostOffice(1, "testgroup");
         
         office2 = (DefaultClusteredPostOffice)createClusteredPostOffice(2, "testgroup");
         
         LocalClusteredQueue queue1 = new LocalClusteredQueue(office1, 1, "queue1", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding1 =
            office1.bindClusteredQueue("topic1", queue1);
         
         LocalClusteredQueue queue2 = new LocalClusteredQueue(office2, 2, "queue2", channelIdManager.getId(), ms, pm, true, true, (QueuedExecutor)pool.get(), null, tr);         
         Binding binding2 =
            office2.bindClusteredQueue("topic1", queue2);
         
         SimpleReceiver receiver1 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue1.add(receiver1);
         SimpleReceiver receiver2 = new SimpleReceiver("blah", SimpleReceiver.ACCEPTING);
         queue2.add(receiver2);
         
         //This will make it fail after casting and persisting the message in the db
         office1.setFail(false, true);
         
         Transaction tx = tr.createTransaction();
         
         final int NUM_MESSAGES = 10;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message msg = CoreMessageFactory.createCoreMessage(i);   
            msg.setReliable(true);
            
            MessageReference ref = ms.reference(msg);  
            
            office1.route(ref, "topic1", tx);
         }
         
         Thread.sleep(1000);
         
         List msgs = receiver1.getMessages();
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertTrue(msgs.isEmpty());
         
         try
         {
            //An exception should be thrown            
            tx.commit();
            fail();                       
         }
         catch (TransactionException e)
         {
            //Ok
         }
         
         Thread.sleep(1000);
         
         msgs = receiver1.getMessages();
         assertTrue(msgs.isEmpty());
         
         msgs = receiver2.getMessages();
         assertTrue(msgs.isEmpty());
         
         //We now kill the office - this should make the other office do it's transaction check
         office1.stop();
         
         Thread.sleep(1000);
         
         assertTrue(office1.getHoldingTransactions().isEmpty());
         
         assertTrue(office2.getHoldingTransactions().isEmpty());
         
         //The tx should be removed from the holding area and messages be received
         //no point checking receiver1 since node1 has crashed
         
         msgs = receiver2.getMessages();
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

}




