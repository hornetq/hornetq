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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.bridge.Bridge;
import org.jboss.logging.Logger;
import org.jboss.tm.TransactionManagerLocator;

/**
 * A BridgeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class BridgeTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(BridgeTest.class);
   
   public BridgeTest(String name)
   {
      super(name);
   }
  
   // MaxBatchSize but no MaxBatchTime
   
   public void testNoMaxBatchTime_AtMostOnce_P() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testNoMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      testNoMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   //Same server
   
   // MaxBatchSize but no MaxBatchTime
   
   public void testNoMaxBatchTimeSameServer_AtMostOnce_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testNoMaxBatchTimeSameServer_DuplicatesOk_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testNoMaxBatchTimeSameServer_OnceAndOnlyOnce_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testNoMaxBatchTimeSameServer_AtMostOnce_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testNoMaxBatchTimeSameServer_DuplicatesOk_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testNoMaxBatchTimeSameServer_OnceAndOnlyOnce_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   
   // MaxBatchTime but no MaxBatchSize
   
   public void testMaxBatchTime_AtMostOnce_P() throws Exception
   {
      this.testMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      this.testMaxBatchTime(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      testMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      this.testMaxBatchTime(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      this.testMaxBatchTime(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      testMaxBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
    
   // Same server
   
   // MaxBatchTime but no MaxBatchSize
   
   public void testMaxBatchTimeSameServer_AtMostOnce_P() throws Exception
   {
      this.testMaxBatchTimeSameServer(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testMaxBatchTimeSameServer_DuplicatesOk_P() throws Exception
   {
      this.testMaxBatchTimeSameServer(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testMaxBatchTimeSameServer_OnceAndOnlyOnce_P() throws Exception
   {
      testMaxBatchTimeSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testMaxBatchTimeSameServer_AtMostOnce_NP() throws Exception
   {
      this.testMaxBatchTimeSameServer(Bridge.QOS_AT_MOST_ONCE, false);
   }
   
   public void testMaxBatchTimeSameServer_DuplicatesOk_NP() throws Exception
   {
      this.testMaxBatchTimeSameServer(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   public void testMaxBatchTimeSameServer_OnceAndOnlyOnce_NP() throws Exception
   {
      testMaxBatchTimeSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   // Stress with batch size of 50
   
   public void testStress_AtMostOnce_P_50() throws Exception
   {
      testStress(Bridge.QOS_AT_MOST_ONCE, true, 50);
   }
   
   public void testStress_DuplicatesOk_P_50() throws Exception
   {
      testStress(Bridge.QOS_DUPLICATES_OK, true, 50);
   }
   
   public void testStress_OnceAndOnlyOnce_P_50() throws Exception
   {
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, true, 50);
   }
   
   public void testStress_AtMostOnce_NP_50() throws Exception
   {
      testStress(Bridge.QOS_AT_MOST_ONCE, false, 50);
   }
   
   public void testStress_DuplicatesOk_NP_50() throws Exception
   {
      testStress(Bridge.QOS_DUPLICATES_OK, false, 50);
   }
   
   public void testStress_OnceAndOnlyOnce_NP_50() throws Exception
   {
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, false, 50);
   }
   
   // Stress with batch size of 1
   
   public void testStress_AtMostOnce_P_1() throws Exception
   {
      testStress(Bridge.QOS_AT_MOST_ONCE, true, 1);
   }
   
   public void testStress_DuplicatesOk_P_1() throws Exception
   {
      testStress(Bridge.QOS_DUPLICATES_OK, true, 1);
   }
   
   public void testStress_OnceAndOnlyOnce_P_1() throws Exception
   {
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, true, 1);
   }
   
   public void testStress_AtMostOnce_NP_1() throws Exception
   {
      testStress(Bridge.QOS_AT_MOST_ONCE, false, 1);
   }
   
   public void testStress_DuplicatesOk_NP_1() throws Exception
   {
      testStress(Bridge.QOS_DUPLICATES_OK, false, 1);
   }
   
   public void testStress_OnceAndOnlyOnce_NP_1() throws Exception
   {
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, false, 1);
   }
   
   // Max batch time
   
   public void testStressMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
   	this.testStressBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, false, 200);
   }
   
   public void testStressMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
   	this.testStressBatchTime(Bridge.QOS_ONCE_AND_ONLY_ONCE, true, 200);
   }
   
   
   // Stress on same server
   
   // Stress with batch size of 50
   
   public void testStressSameServer_AtMostOnce_P_50() throws Exception
   {
      testStressSameServer(Bridge.QOS_AT_MOST_ONCE, true, 50);
   }
   
   public void testStressSameServer_DuplicatesOk_P_50() throws Exception
   {
      testStressSameServer(Bridge.QOS_DUPLICATES_OK, true, 50);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_P_50() throws Exception
   {
      testStress(Bridge.QOS_ONCE_AND_ONLY_ONCE, true, 50);
   }
   
   public void testStressSameServer_AtMostOnce_NP_50() throws Exception
   {
      testStressSameServer(Bridge.QOS_AT_MOST_ONCE, false, 50);
   }
   
   public void testStressSameServer_DuplicatesOk_NP_50() throws Exception
   {
      testStressSameServer(Bridge.QOS_DUPLICATES_OK, false, 50);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_NP_50() throws Exception
   {
      testStressSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, false, 50);
   }
   
   // Stress with batch size of 1
   
   public void testStressSameServer_AtMostOnce_P_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_AT_MOST_ONCE, true, 1);
   }
   
   public void testStressSameServer_DuplicatesOk_P_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_DUPLICATES_OK, true, 1);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_P_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, true, 1);
   }
   
   public void testStressSameServer_AtMostOnce_NP_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_AT_MOST_ONCE, false, 1);
   }
   
   public void testStressSameServer_DuplicatesOk_NP_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_DUPLICATES_OK, false, 1);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_NP_1() throws Exception
   {
      testStressSameServer(Bridge.QOS_ONCE_AND_ONLY_ONCE, false, 1);
   }
   
   public void testParams() throws Exception
   {
      Bridge bridge = null;
      
      try
      {               
         int qosMode = Bridge.QOS_AT_MOST_ONCE;
         
         int batchSize = 10;
         
         int maxBatchTime = -1;
         
         String sourceUsername = null;
         
         String sourcePassword = null;
         
         String destUsername = null;
         
         String destPassword = null;
         
         String selector = null;
         
         long failureRetryInterval = 5000;
         
         int maxRetries = 10;
         
         String subName = null;
         
         String clientID = null;
         
         try
         {
            bridge= new Bridge(null, cff1, sourceQueue, destQueue,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, null, sourceQueue, destQueue,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, null, destQueue,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, destQueue,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, -2, maxRetries, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, destQueue,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, -1, 10, qosMode,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, -2,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, 3,
                               batchSize, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, 3,
                               0, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new Bridge(cff0, cff1, sourceQueue, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, 3,
                               batchSize, -2,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
      }
      finally
      {                      
         if (bridge != null)
         {
            bridge.stop();
         }
      }         
   }
   
   public void testSelector() throws Exception
   {      
      Bridge bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         String selector = "vegetable='radish'";
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  selector, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            if (i >= NUM_MESSAGES / 2)
            {
               tm.setStringProperty("vegetable", "radish");
            }
            else
            {
               tm.setStringProperty("vegetable", "cauliflower");
            }
            
            prod.send(tm);
         }
         
         connTarget = cf1.createConnection();
         
         Session sessRec = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connTarget.start();
                                 
         for (int i = NUM_MESSAGES / 2 ; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons.receive(1000);
         
         assertNull(m);
                       
      }
      finally
      {      
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
         
         if (bridge != null)
         {
            bridge.stop();
         }
         
         removeAllMessages(sourceQueue.getQueueName(), true, 0);
      }                  
   }
   
   public void testStartBridgeWithJTATransactionAlreadyRunning() throws Exception
   {  
      Bridge bridge = null;
      
      Transaction toResume = null;
      
      Transaction started = null;
      
      TransactionManager mgr = TransactionManagerLocator.getInstance().locate();
                  
      try
      {
         
         toResume = mgr.suspend();
         
         mgr.begin();
         
         started = mgr.getTransaction();         
           
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceTopic, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         
         bridge.start();
         
         this.sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, false);
            
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);                          
      }
      finally
      {      
         if (started != null)
         {
            try
            {
               started.rollback();
            }
            catch (Exception e)
            {
               log.error("Failed to rollback", e);
            }
         }
         
         if (toResume != null)
         {
            try
            {
               mgr.resume(toResume);
            }
            catch (Exception e)
            {
               log.error("Failed to resume", e);
            }
         }         
         if (bridge != null)
         {
            bridge.stop();
         }     
      }                  
   }   
   
   public void testNonDurableSubscriber() throws Exception
   { 
      Bridge bridge = null;
            
      try
      {   
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceTopic, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         
         bridge.start();
            
         sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, false);
         
         checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);                    
      }
      finally
      {                        
         if (bridge != null)
         {
            bridge.stop();
         }
      }                  
   }
   
   public void testDurableSubscriber() throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceTopic, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  "subTest", "clientid123", false);
         
         bridge.start();
            
         sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, true);
         
         checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);              
      }
      finally
      {                      
         if (bridge != null)
         {
            bridge.stop();
         }
         
         //Now unsubscribe
         Connection conn = cf0.createConnection();
         conn.setClientID("clientid123");
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sess.unsubscribe("subTest");
         conn.close();         
      }                  
   }
      
   public void testMessageIDInHeaderOn() throws Exception
   {
   	messageIDInHeader(true);
   }
   
   public void testMessageIDInHeaderOff() throws Exception
   {
   	messageIDInHeader(false);
   }
   
   private void messageIDInHeader(boolean on) throws Exception
   { 
      Bridge bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, on);
         
         bridge.start();
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
                    
         log.trace("Sending " + NUM_MESSAGES + " messages");
         
         List ids1 = new ArrayList();
     
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSource.createProducer(sourceQueue);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            //We add some headers to make sure they get passed through ok
            tm.setStringProperty("wib", "uhuh");
            tm.setBooleanProperty("cheese", true);
            tm.setIntProperty("Sausages", 23);
            
            //We add some JMSX ones too
            
            tm.setStringProperty("JMSXGroupID", "mygroup543");
            tm.setIntProperty("JMSXGroupSeq", 777);
            
            prod.send(tm);
            
            ids1.add(tm.getJMSMessageID());
         }

         log.trace("Sent the first messages");
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(destQueue);
         
         connTarget.start();
         
         List msgs = new ArrayList();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(5000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
            
            assertEquals("uhuh", tm.getStringProperty("wib"));
            assertTrue(tm.getBooleanProperty("cheese"));
            assertEquals(23, tm.getIntProperty("Sausages"));
            
            assertEquals("mygroup543", tm.getStringProperty("JMSXGroupID"));
            assertEquals(777, tm.getIntProperty("JMSXGroupSeq"));
            
            if (on)
            {            
	            String header = tm.getStringProperty(JBossMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
	            
	            assertNotNull(header);
	            
	            assertEquals(ids1.get(i), header);
	            
	            msgs.add(tm);
            }
         }
         
         if (on)
         {	         
	         //Now we send them again back to the source
	         
	         Iterator iter = msgs.iterator();
	         
	         List ids2 = new ArrayList();
	         
	         while (iter.hasNext())
	         {
	         	Message msg = (Message)iter.next();
	         	
	         	prod.send(msg);
	            
	            ids2.add(msg.getJMSMessageID());
	         }
	                               
	         //And consume them again
	         
	         for (int i = 0; i < NUM_MESSAGES; i++)
	         {
	            TextMessage tm = (TextMessage)cons.receive(5000);
	            
	            assertNotNull(tm);
	            
	            assertEquals("message" + i, tm.getText());
	            
	            assertEquals("uhuh", tm.getStringProperty("wib"));
	            assertTrue(tm.getBooleanProperty("cheese"));
	            assertEquals(23, tm.getIntProperty("Sausages"));
	            
	            assertEquals("mygroup543", tm.getStringProperty("JMSXGroupID"));
	            assertEquals(777, tm.getIntProperty("JMSXGroupSeq"));            
	            
	            String header = tm.getStringProperty(JBossMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
	            
	            assertNotNull(header);
	            
	            assertEquals(ids1.get(i) + "," + ids2.get(i), header);
	         }
         }
         
      }
      finally
      {                        
         if (bridge != null)
         {
            bridge.stop();
         }
         
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
      }                  
   }
   
   
   public void testPropertiesPreservedPOn() throws Exception
   {
   	propertiesPreserved(true, true);
   }
   
   public void testPropertiesPreservedNPoff() throws Exception
   {
   	propertiesPreserved(false, true);
   }
   
   public void testPropertiesPreservedNPOn() throws Exception
   {
   	propertiesPreserved(false, true);
   }
   
   public void testPropertiesPreservedPoff() throws Exception
   {
   	propertiesPreserved(true, true);
   }
   
   private void propertiesPreserved(boolean persistent, boolean messageIDInHeader) throws Exception
   { 
      Bridge bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, messageIDInHeader);
         
         bridge.start();
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
                    
         log.trace("Sending " + NUM_MESSAGES + " messages");
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(destQueue);
         
         connTarget.start();
         
         MessageProducer prod = sessSource.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);  
         
         
         
         TextMessage tm = sessSource.createTextMessage("blahmessage");
         
         prod.setPriority(7);
         
         prod.setTimeToLive(1 * 60 * 60 * 1000);

         prod.send(tm);
         
         long expiration = tm.getJMSExpiration();
         
         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
                                 
         tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("blahmessage", tm.getText());

         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
         assertEquals(7, tm.getJMSPriority());
        
         assertTrue(Math.abs(expiration - tm.getJMSExpiration()) < 100);
                  
         Message m = cons.receive(5000);
         
         assertNull(m);
         
         
         //Now do one with expiration = 0
         
         
         tm = sessSource.createTextMessage("blahmessage2");
         
         prod.setPriority(7);
         
         prod.setTimeToLive(0);

         prod.send(tm);
         
         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
                                 
         tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("blahmessage2", tm.getText());

         assertEquals(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, tm.getJMSDeliveryMode());
         
         assertEquals(7, tm.getJMSPriority());
        
         assertEquals(0, tm.getJMSExpiration());
                  
         m = cons.receive(5000);
         
         assertNull(m);                          
         
      }
      finally
      {                        
         if (bridge != null)
         {
            bridge.stop();
         }
         
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
      }                  
   }
   
   public void testNoMessageIDInHeader() throws Exception
   { 
      Bridge bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, Bridge.QOS_AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         
         bridge.start();
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
                    
         log.trace("Sending " + NUM_MESSAGES + " messages");
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSource.createProducer(sourceQueue);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            //We add some headers to make sure they get passed through ok
            tm.setStringProperty("wib", "uhuh");
            tm.setBooleanProperty("cheese", true);
            tm.setIntProperty("Sausages", 23);
            
            prod.send(tm);
         }

         log.trace("Sent the first messages");
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(destQueue);
         
         connTarget.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(5000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
            
            assertEquals("uhuh", tm.getStringProperty("wib"));
            assertTrue(tm.getBooleanProperty("cheese"));
            assertEquals(23, tm.getIntProperty("Sausages"));
            
            String header = tm.getStringProperty(JBossMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
            
            assertNull(header);
         }                 
      }
      finally
      {                        
         if (bridge != null)
         {
            bridge.stop();
         }
         
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
      }                  
   }
   
   
   // Private -------------------------------------------------------------------------------
             
   private void testStress(int qosMode, boolean persistent, int batchSize) throws Exception
   {
      Connection connSource = null;
      
      Bridge bridge = null;
      
      Thread t = null;
            
      try
      {      
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  batchSize, -1,
                  null, null, false);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         final int NUM_MESSAGES = 250;
         
         StressSender sender = new StressSender();
         sender.sess = sessSend;
         sender.prod = prod;
         sender.numMessages = NUM_MESSAGES;
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                          
         t = new Thread(sender);
         
         t.start();
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);
                                              
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
         
         if (bridge != null)
         {
            bridge.stop();
         }                  
      }      
   }
   
   private void testStressBatchTime(int qosMode, boolean persistent, int maxBatchTime) throws Exception
   {
      Connection connSource = null;
      
      Bridge bridge = null;
      
      Thread t = null;
            
      try
      {      
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  2, maxBatchTime,
                  null, null, false);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         final int NUM_MESSAGES = 5000;
         
         StressSender sender = new StressSender();
         sender.sess = sessSend;
         sender.prod = prod;
         sender.numMessages = NUM_MESSAGES;
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                          
         t = new Thread(sender);
         
         t.start();
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);
                                              
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
         
         if (bridge != null)
         {
            bridge.stop();
         }                  
      }      
   }
   
   //Both source and destination on same rm
   private void testStressSameServer(int qosMode, boolean persistent, int batchSize) throws Exception
   {
      Connection connSource = null;
      
      Bridge bridge = null;
      
      Thread t = null;
            
      try
      {  
         bridge = new Bridge(cff0, cff0, sourceQueue, localDestQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  batchSize, -1,
                  null, null, false);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         final int NUM_MESSAGES = 2000;
         
         StressSender sender = new StressSender();
         sender.sess = sessSend;
         sender.prod = prod;
         sender.numMessages = NUM_MESSAGES;
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                          
         t = new Thread(sender);
         
         t.start();
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, 0, NUM_MESSAGES);
                         
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
                          
         if (bridge != null)
         {
            bridge.stop();
         }
      }      
   }
   
      
   private void testNoMaxBatchTime(int qosMode, boolean persistent) throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  NUM_MESSAGES, -1,
                  null, null, false);
         
         bridge.start();
            
         //Send half the messges

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
                         
         //Verify none are received
         
         this.checkEmpty(destQueue, 1);
         
         //Send the other half
         
         this.sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
         
         //This should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);
         
         //Send another batch with one more than batch size
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES + 1, persistent);
                  
         //Make sure only batch size are received
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);
         
         //Final batch
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES - 1, persistent);
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, NUM_MESSAGES, 1);
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES - 1);
      }
      finally
      {      
         if (bridge != null)
         {
            log.info("Stopping bridge");
            bridge.stop();
         }         
      }                  
   }
   
   private void testNoMaxBatchTimeSameServer(int qosMode, boolean persistent) throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new Bridge(cff0, cff0, sourceQueue, localDestQueue,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  NUM_MESSAGES, -1,
                  null, null, false);
         
         bridge.start();
            
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
         
         this.checkEmpty(destQueue, 1);                
         
         //Send the other half
         
         this.sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES /2, persistent);
         
         
         //This should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, 0, NUM_MESSAGES);
         
         this.checkEmpty(localDestQueue, 0);
         
         this.checkEmpty(sourceQueue, 0);
         
         //Send another batch with one more than batch size
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES + 1, persistent);
         
         //Make sure only batch size are received
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, 0, NUM_MESSAGES);
         
         //Final batch
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES - 1, persistent);
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, NUM_MESSAGES, 1);
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, 0, NUM_MESSAGES - 1);
      }
      finally
      {               
         if (bridge != null)
         {
            bridge.stop();
         }                  
      }                  
   }
   
   private void testMaxBatchTime(int qosMode, boolean persistent) throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         final long MAX_BATCH_TIME = 3000;
         
         final int MAX_BATCH_SIZE = 100000; // something big so it won't reach it
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 3000, 10, qosMode,
                  MAX_BATCH_SIZE, MAX_BATCH_TIME,
                  null, null, false);
         
         bridge.start();
            
         final int NUM_MESSAGES = 10;
         
         //Send some message

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES, persistent);                 
         
         //Verify none are received
         
         this.checkEmpty(destQueue, 1);
         
         //Messages should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf1, destQueue, 0, NUM_MESSAGES);         
      }
      finally
      {      
         if (bridge != null)
         {
            bridge.stop();
         }         
      }                  
   }
   
   private void testMaxBatchTimeSameServer(int qosMode, boolean persistent) throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         final long MAX_BATCH_TIME = 3000;
         
         final int MAX_BATCH_SIZE = 100000; // something big so it won't reach it
         
         bridge = new Bridge(cff0, cff0, sourceQueue, localDestQueue,
                  null, null, null, null,
                  null, 3000, 10, qosMode,
                  MAX_BATCH_SIZE, MAX_BATCH_TIME,
                  null, null, false);
         
         bridge.start();
         
         final int NUM_MESSAGES = 10;
         
         //Send some message

         //Send some message

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES, persistent);                 
         
         //Verify none are received
         
         this.checkEmpty(localDestQueue, 0);;
         
         //Messages should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf0, localDestQueue, 0, NUM_MESSAGES);
      }
      finally
      {              
         if (bridge != null)
         {
            bridge.stop();
         }        
      }                  
   }
   
   
   // Inner classes -------------------------------------------------------------------
   
   private static class StressSender implements Runnable
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
}
