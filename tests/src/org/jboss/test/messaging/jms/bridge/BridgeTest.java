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
package org.jboss.test.messaging.jms.bridge;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.server.bridge.Bridge;
import org.jboss.jms.server.bridge.ConnectionFactoryFactory;
import org.jboss.jms.server.bridge.JNDIConnectionFactoryFactory;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A BridgeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeTest extends MessagingTestCase
{
   private static final Logger log = Logger.getLogger(BridgeTest.class);
   
   public BridgeTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   // MaxBatchSize but no MaxBatchTime
   
   public void testNoMaxBatchTime_AtMostOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testNoMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testNoMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   
   // MaxBatchTime but no MaxBatchSize
   
   public void testMaxBatchTime_AtMostOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      this.testMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      this.testMaxBatchTime(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      this.testMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      this.testMaxBatchTime(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
    
   
   // Stress 
   
   public void testStress_AtMostOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testStress_DuplicatesOk_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testStress_OnceAndOnlyOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testStress_AtMostOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testStress_DuplicatesOk_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testStress_OnceAndOnlyOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   
   
   private static class Sender implements Runnable
   {
      int numMessages;
      
      Session sess;
      
      MessageProducer prod;
      
      Exception ex;
      
      public void run()
      {
         try
         {
            for (int i = 0; i < numMessages; i++)
            {
               TextMessage tm = sess.createTextMessage("message" + i);
                                            
               prod.send(tm);
               
               log.trace("Sent message " + i);
            }
         }
         catch (Exception e)
         {
            log.error("Failed to send", e);
            ex = e;
         }         
      }
      
   }   
   
   private void testStress(int qosMode, boolean persistent) throws Exception
   {
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
      
      Thread t = null;
            
      try
      {
         ServerManagement.start(0, "all", null, true);
         
         ServerManagement.start(1, "all", null, false);
         
         ServerManagement.deployQueue("sourceQueue", 0);
         
         ServerManagement.deployQueue("destQueue", 1);
         
         Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
         
         ConnectionFactoryFactory cff0 = new JNDIConnectionFactoryFactory(props0, "/ConnectionFactory");
         
         ConnectionFactoryFactory cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
                      
         InitialContext ic0 = new InitialContext(props0);
         
         InitialContext ic1 = new InitialContext(props1);
         
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
         
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
         
         Queue sourceQueue = (Queue)ic0.lookup("/queue/sourceQueue");
         
         Queue destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         final int BATCH_SIZE = 50;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  BATCH_SIZE, -1,
                  null, null);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         final int NUM_MESSAGES = 2000;
         
         Sender sender = new Sender();
         sender.sess = sessSend;
         sender.prod = prod;
         sender.numMessages = NUM_MESSAGES;
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                          
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         t = new Thread(sender);
         
         t.start();
                 
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(5000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons.receive(1000);
         
         assertNull(m);
         
         t.join();
         
         if (sender.ex != null)
         {
            //An error occurred during the send
            throw sender.ex;
         }
           
      }
      finally
      {    
         if (t != null)
         {
            t.join(10000);
         }
         
         if (connSource != null)
         {
            try
            {
               connSource.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
            }
         }
         
         if (connDest != null)
         {
            try
            {
               connDest.close();
            }
            catch (Exception e)
            {
              log.error("Failed to close connection", e);
            }
         }
         
         if (bridge != null)
         {
            bridge.stop();
         }
         
         try
         {
            ServerManagement.undeployQueue("sourceQueue", 0);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         try
         {
            ServerManagement.undeployQueue("destQueue", 1);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         ServerManagement.stop(0);
         
         ServerManagement.stop(1);
      }      
   }
   
      
   private void testNoMaxBatchTime(int qosMode, boolean persistent) throws Exception
   {
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
            
      try
      {
         ServerManagement.start(0, "all", null, true);
         
         ServerManagement.start(1, "all", null, false);
         
         ServerManagement.deployQueue("sourceQueue", 0);
         
         ServerManagement.deployQueue("destQueue", 1);
         
         Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
         
         ConnectionFactoryFactory cff0 = new JNDIConnectionFactoryFactory(props0, "/ConnectionFactory");
         
         ConnectionFactoryFactory cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
                      
         InitialContext ic0 = new InitialContext(props0);
         
         InitialContext ic1 = new InitialContext(props1);
         
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
         
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
         
         Queue sourceQueue = (Queue)ic0.lookup("/queue/sourceQueue");
         
         Queue destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         final int BATCH_SIZE = 10;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  BATCH_SIZE, -1,
                  null, null);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);        
         
         //Send half the messges

         for (int i = 0; i < BATCH_SIZE / 2; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         //Verify none are received
         
         Message m = cons.receive(2000);
         
         assertNull(m);
         
         //Send the other half
         
         for (int i = BATCH_SIZE / 2; i < BATCH_SIZE; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //This should now be receivable
         
         for (int i = 0; i < BATCH_SIZE; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         //Send another batch with one more than batch size
         
         for (int i = 0; i < BATCH_SIZE + 1; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //Make sure only batch size are received
         
         for (int i = 0; i < BATCH_SIZE; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(2000);
         
         assertNull(m);
         
         //Final batch
         
         for (int i = 0; i < BATCH_SIZE - 1; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         TextMessage tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("message" + BATCH_SIZE, tm.getText());
         
         for (int i = 0; i < BATCH_SIZE - 1; i++)
         {
            tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         
         //Make sure no messages are left in the source dest
         
         MessageConsumer cons2 = sessSend.createConsumer(sourceQueue);
         
         connSource.start();
         
         m = cons2.receive(1000);
         
         assertNull(m);          
      }
      finally
      {      
         if (connSource != null)
         {
            try
            {
               connSource.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
            }
         }
         
         if (connDest != null)
         {
            try
            {
               connDest.close();
            }
            catch (Exception e)
            {
              log.error("Failed to close connection", e);
            }
         }
         
         if (bridge != null)
         {
            bridge.stop();
         }
         
         try
         {
            ServerManagement.undeployQueue("sourceQueue", 0);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         try
         {
            ServerManagement.undeployQueue("destQueue", 1);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         ServerManagement.stop(0);
         
         ServerManagement.stop(1);
      }                  
   }
   
   private void testMaxBatchTime(int qosMode, boolean persistent) throws Exception
   {
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
            
      try
      {
         ServerManagement.start(0, "all", null, true);
         
         ServerManagement.start(1, "all", null, false);
         
         ServerManagement.deployQueue("sourceQueue", 0);
         
         ServerManagement.deployQueue("destQueue", 1);
         
         Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
         
         ConnectionFactoryFactory cff0 = new JNDIConnectionFactoryFactory(props0, "/ConnectionFactory");
         
         ConnectionFactoryFactory cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
               
         InitialContext ic0 = new InitialContext(props0);
         
         InitialContext ic1 = new InitialContext(props1);
         
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
         
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
         
         Queue sourceQueue = (Queue)ic0.lookup("/queue/sourceQueue");
         
         Queue destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         final long MAX_BATCH_TIME = 3000;
         
         final int MAX_BATCH_SIZE = 100000; // something big so it won't reach it
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  MAX_BATCH_SIZE, MAX_BATCH_TIME,
                  null, null);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);                          
         
         final int NUM_MESSAGES = 10;
         
         //Send some message

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         //Verify none are received
         
         Message m = cons.receive(2000);
         
         assertNull(m);
         
         //Wait a bit longer
         
         Thread.sleep(1500);
         
         //Messages should now be receivable
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(1000);
         
         assertNull(m);
         
         //Make sure no messages are left in the source dest
         
         MessageConsumer cons2 = sessSend.createConsumer(sourceQueue);
         
         connSource.start();
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
      }
      finally
      {      
         if (connSource != null)
         {
            try
            {
               connSource.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
            }
         }
         
         if (connDest != null)
         {
            try
            {
               connDest.close();
            }
            catch (Exception e)
            {
              log.error("Failed to close connection", e);
            }
         }
         
         if (bridge != null)
         {
            bridge.stop();
         }
         
         try
         {
            ServerManagement.undeployQueue("sourceQueue", 0);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         try
         {
            ServerManagement.undeployQueue("destQueue", 1);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         ServerManagement.stop(0);
         
         ServerManagement.stop(1);
      }                  
   }
}
