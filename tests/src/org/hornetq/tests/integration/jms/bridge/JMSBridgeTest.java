/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.jms.bridge;

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

import org.hornetq.core.logging.Logger;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.bridge.impl.JMSBridgeImpl;
import org.hornetq.jms.client.HornetQMessage;

/**
 * A JMSBridgeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class JMSBridgeTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(JMSBridgeTest.class);
   
   // MaxBatchSize but no MaxBatchTime
   
   public void testNoMaxBatchTime_AtMostOnce_P() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.AT_MOST_ONCE, true);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.DUPLICATES_OK, true);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testNoMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.AT_MOST_ONCE, false);
   }
   
   public void testNoMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.DUPLICATES_OK, false);
   }
   
   public void testNoMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      testNoMaxBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false);
   }
   
   //Same server
   
   // MaxBatchSize but no MaxBatchTime
   
   public void testNoMaxBatchTimeSameServer_AtMostOnce_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.AT_MOST_ONCE, true);
   }
   
   public void testNoMaxBatchTimeSameServer_DuplicatesOk_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.DUPLICATES_OK, true);
   }
   
   public void testNoMaxBatchTimeSameServer_OnceAndOnlyOnce_P() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testNoMaxBatchTimeSameServer_AtMostOnce_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.AT_MOST_ONCE, false);
   }
   
   public void testNoMaxBatchTimeSameServer_DuplicatesOk_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.DUPLICATES_OK, false);
   }
   
   public void testNoMaxBatchTimeSameServer_OnceAndOnlyOnce_NP() throws Exception
   {
      testNoMaxBatchTimeSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false);
   }
   
   
   // MaxBatchTime but no MaxBatchSize
   
   public void testMaxBatchTime_AtMostOnce_P() throws Exception
   {
      this.testMaxBatchTime(QualityOfServiceMode.AT_MOST_ONCE, true);
   }
   
   public void testMaxBatchTime_DuplicatesOk_P() throws Exception
   {
      this.testMaxBatchTime(QualityOfServiceMode.DUPLICATES_OK, true);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
      testMaxBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testMaxBatchTime_AtMostOnce_NP() throws Exception
   {
      this.testMaxBatchTime(QualityOfServiceMode.AT_MOST_ONCE, false);
   }
   
   public void testMaxBatchTime_DuplicatesOk_NP() throws Exception
   {
      this.testMaxBatchTime(QualityOfServiceMode.DUPLICATES_OK, false);
   }
   
   public void testMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
      testMaxBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false);
   }
    
   // Same server
   
   // MaxBatchTime but no MaxBatchSize
   
   public void testMaxBatchTimeSameServer_AtMostOnce_P() throws Exception
   {
      this.testMaxBatchTimeSameServer(QualityOfServiceMode.AT_MOST_ONCE, true);
   }
   
   public void testMaxBatchTimeSameServer_DuplicatesOk_P() throws Exception
   {
      this.testMaxBatchTimeSameServer(QualityOfServiceMode.DUPLICATES_OK, true);
   }
   
   public void testMaxBatchTimeSameServer_OnceAndOnlyOnce_P() throws Exception
   {
      testMaxBatchTimeSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testMaxBatchTimeSameServer_AtMostOnce_NP() throws Exception
   {
      this.testMaxBatchTimeSameServer(QualityOfServiceMode.AT_MOST_ONCE, false);
   }
   
   public void testMaxBatchTimeSameServer_DuplicatesOk_NP() throws Exception
   {
      this.testMaxBatchTimeSameServer(QualityOfServiceMode.DUPLICATES_OK, false);
   }
   
   public void testMaxBatchTimeSameServer_OnceAndOnlyOnce_NP() throws Exception
   {
      testMaxBatchTimeSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false);
   }
   
   // Stress with batch size of 50
   
   public void testStress_AtMostOnce_P_50() throws Exception
   {
      testStress(QualityOfServiceMode.AT_MOST_ONCE, true, 50);
   }
   
   public void testStress_DuplicatesOk_P_50() throws Exception
   {
      testStress(QualityOfServiceMode.DUPLICATES_OK, true, 50);
   }
   
   public void testStress_OnceAndOnlyOnce_P_50() throws Exception
   {
      testStress(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, 50);
   }
   
   public void testStress_AtMostOnce_NP_50() throws Exception
   {
      testStress(QualityOfServiceMode.AT_MOST_ONCE, false, 50);
   }
   
   public void testStress_DuplicatesOk_NP_50() throws Exception
   {
      testStress(QualityOfServiceMode.DUPLICATES_OK, false, 50);
   }
   
   public void testStress_OnceAndOnlyOnce_NP_50() throws Exception
   {
      testStress(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, 50);
   }
   
   // Stress with batch size of 1
   
   public void testStress_AtMostOnce_P_1() throws Exception
   {
      testStress(QualityOfServiceMode.AT_MOST_ONCE, true, 1);
   }
   
   public void testStress_DuplicatesOk_P_1() throws Exception
   {
      testStress(QualityOfServiceMode.DUPLICATES_OK, true, 1);
   }
   
   public void testStress_OnceAndOnlyOnce_P_1() throws Exception
   {
      testStress(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, 1);
   }
   
   public void testStress_AtMostOnce_NP_1() throws Exception
   {
      testStress(QualityOfServiceMode.AT_MOST_ONCE, false, 1);
   }
   
   public void testStress_DuplicatesOk_NP_1() throws Exception
   {
      testStress(QualityOfServiceMode.DUPLICATES_OK, false, 1);
   }
   
   public void testStress_OnceAndOnlyOnce_NP_1() throws Exception
   {
      testStress(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, 1);
   }
   
   // Max batch time
   
   public void testStressMaxBatchTime_OnceAndOnlyOnce_NP() throws Exception
   {
   	this.testStressBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, 200);
   }
   
   public void testStressMaxBatchTime_OnceAndOnlyOnce_P() throws Exception
   {
   	this.testStressBatchTime(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, 200);
   }
   
   
   // Stress on same server
   
   // Stress with batch size of 50
   
   public void testStressSameServer_AtMostOnce_P_50() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.AT_MOST_ONCE, true, 50);
   }
   
   public void testStressSameServer_DuplicatesOk_P_50() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.DUPLICATES_OK, true, 50);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_P_50() throws Exception
   {
      testStress(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, 50);
   }
   
   public void testStressSameServer_AtMostOnce_NP_50() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.AT_MOST_ONCE, false, 50);
   }
   
   public void testStressSameServer_DuplicatesOk_NP_50() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.DUPLICATES_OK, false, 50);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_NP_50() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, 50);
   }
   
   // Stress with batch size of 1
   
   public void testStressSameServer_AtMostOnce_P_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.AT_MOST_ONCE, true, 1);
   }
   
   public void testStressSameServer_DuplicatesOk_P_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.DUPLICATES_OK, true, 1);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_P_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, 1);
   }
   
   public void testStressSameServer_AtMostOnce_NP_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.AT_MOST_ONCE, false, 1);
   }
   
   public void testStressSameServer_DuplicatesOk_NP_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.DUPLICATES_OK, false, 1);
   }
   
   public void testStressSameServer_OnceAndOnlyOnce_NP_1() throws Exception
   {
      testStressSameServer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, 1);
   }
   
   public void testParams() throws Exception
   {
      JMSBridgeImpl bridge = null;
      
      try
      {               
         QualityOfServiceMode qosMode = QualityOfServiceMode.AT_MOST_ONCE;
         
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
            bridge= new JMSBridgeImpl(null, cff1, sourceQueueFactory, targetQueueFactory,
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
            bridge= new JMSBridgeImpl(cff0, null, sourceQueueFactory, targetQueueFactory,
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
            bridge= new JMSBridgeImpl(cff0, cff1, null, targetQueueFactory,
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
            bridge= new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, null,
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
            bridge= new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
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
            bridge= new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
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
            bridge= new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
                               0, maxBatchTime,
                               subName, clientID, false);
         }
         catch (IllegalArgumentException e)
         {
            //Ok
         }
         
         try
         {
            bridge= new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, null,
                               sourceUsername, sourcePassword, destUsername, destPassword,
                               selector, failureRetryInterval, maxRetries, qosMode,
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
      JMSBridgeImpl bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         String selector = "vegetable='radish'";
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  selector, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

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
         
         MessageConsumer cons = sessRec.createConsumer(targetQueue);
         
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
         
         removeAllMessages(sourceQueue.getQueueName(), 0);
      }                  
   }
   
   
   
   public void testStartBridgeWithJTATransactionAlreadyRunningLargeMessage() throws Exception
   {
      internalTestStartBridgeWithJTATransactionAlreadyRunning(true);
   }
   
   public void testStartBridgeWithJTATransactionAlreadyRunningRegularMessage() throws Exception
   {
      internalTestStartBridgeWithJTATransactionAlreadyRunning(false);
   }
   
   public void internalTestStartBridgeWithJTATransactionAlreadyRunning(boolean largeMessage) throws Exception
   {  
      JMSBridgeImpl bridge = null;
      
      Transaction toResume = null;
      
      Transaction started = null;
      
      TransactionManager mgr = newTransactionManager();
                  
      try
      {
         
         toResume = mgr.suspend();
         
         mgr.begin();
         
         started = mgr.getTransaction();         
           
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceTopicFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         bridge.setTransactionManager(mgr);
         bridge.start();
         
         this.sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, false, largeMessage);
            
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, largeMessage);                          
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
   public void testNonDurableSubscriberLargeMessage() throws Exception
   {
      internalTestNonDurableSubscriber(true, 1);
   }
   
   public void testNonDurableSubscriberRegularMessage() throws Exception
   {
      internalTestNonDurableSubscriber(false, 1);
   }
   
   public void internalTestNonDurableSubscriber(boolean largeMessage, int batchSize) throws Exception
   { 
      JMSBridgeImpl bridge = null;
            
      try
      {   
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceTopicFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  batchSize, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
            
         sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, false, largeMessage);
         
         checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, largeMessage);                    
      }
      finally
      {                        
         if (bridge != null)
         {
            bridge.stop();
         }
      }                  
   }

   public void testDurableSubscriberLargeMessage() throws Exception
   {
      internalTestDurableSubscriber(true, 1);
   }
   
   public void testDurableSubscriberRegularMessage() throws Exception
   {
      internalTestDurableSubscriber(false, 1);
   }

   public void internalTestDurableSubscriber(boolean largeMessage, int batchSize) throws Exception
   {
      JMSBridgeImpl bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceTopicFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  batchSize, -1,
                  "subTest", "clientid123", false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
            
         sendMessages(cf0, sourceTopic, 0, NUM_MESSAGES, true, largeMessage);
         
         checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, largeMessage);              
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
      JMSBridgeImpl bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  1, -1,
                  null, null, on);
         bridge.setTransactionManager(newTransactionManager());

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
         
         MessageConsumer cons = sessTarget.createConsumer(targetQueue);
         
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
	            String header = tm.getStringProperty(HornetQMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
	            
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
	            
	            String header = tm.getStringProperty(HornetQMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
	            
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
      JMSBridgeImpl bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  1, -1,
                  null, null, messageIDInHeader);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
                    
         log.trace("Sending " + NUM_MESSAGES + " messages");
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(targetQueue);
         
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
         
         tm = sessSource.createTextMessage("blahmessage3");

         final boolean myBool = false;
         final byte myByte = (byte)23;
         final double myDouble = 17625765d;
         final float myFloat = 87127.23f;
         final int myInt = 123;
         final long myLong = 81728712;
         final short myShort = (short)88;
         final String myString = "ojweodewj";
         final String myJMSX = "aardvark";

         tm.setBooleanProperty("mybool", myBool);
         tm.setByteProperty("mybyte", myByte);
         tm.setDoubleProperty("mydouble", myDouble);
         tm.setFloatProperty("myfloat", myFloat);
         tm.setIntProperty("myint", myInt);
         tm.setLongProperty("mylong", myLong);
         tm.setShortProperty("myshort", myShort);
         tm.setStringProperty("mystring", myString);

         tm.setStringProperty("JMSXMyNaughtyJMSXProperty", myJMSX);

         prod.send(tm);

         tm = (TextMessage)cons.receive(5000);

         assertNotNull(tm);

         assertEquals("blahmessage3", tm.getText());

         assertEquals(myBool, tm.getBooleanProperty("mybool"));
         assertEquals(myByte, tm.getByteProperty("mybyte"));
         assertEquals(myDouble, tm.getDoubleProperty("mydouble"));
         assertEquals(myFloat, tm.getFloatProperty("myfloat"));
         assertEquals(myInt, tm.getIntProperty("myint"));
         assertEquals(myLong, tm.getLongProperty("mylong"));
         assertEquals(myShort, tm.getShortProperty("myshort"));
         assertEquals(myString, tm.getStringProperty("mystring"));
         assertEquals(myJMSX, tm.getStringProperty("JMSXMyNaughtyJMSXProperty"));

         m = cons.receive(5000);
         
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
      JMSBridgeImpl bridge = null;
      
      Connection connSource = null;
      
      Connection connTarget = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  1, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

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
         
         MessageConsumer cons = sessTarget.createConsumer(targetQueue);
         
         connTarget.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(5000);
            
            assertNotNull(tm);
            
            assertEquals("message" + i, tm.getText());
            
            assertEquals("uhuh", tm.getStringProperty("wib"));
            assertTrue(tm.getBooleanProperty("cheese"));
            assertEquals(23, tm.getIntProperty("Sausages"));
            
            String header = tm.getStringProperty(HornetQMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
            
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
             
   private void testStress(QualityOfServiceMode qosMode, boolean persistent, int batchSize) throws Exception
   {
      Connection connSource = null;
      
      JMSBridgeImpl bridge = null;
      
      Thread t = null;
            
      try
      {      
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  batchSize, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

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
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, false);
                                              
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
   
   private void testStressBatchTime(QualityOfServiceMode qosMode, boolean persistent, int maxBatchTime) throws Exception
   {
      Connection connSource = null;
      
      JMSBridgeImpl bridge = null;
      
      Thread t = null;
            
      try
      {      
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  2, maxBatchTime,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

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
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, false);
                                              
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
   private void testStressSameServer(QualityOfServiceMode qosMode, boolean persistent, int batchSize) throws Exception
   {
      Connection connSource = null;
      
      JMSBridgeImpl bridge = null;
      
      Thread t = null;
            
      try
      {  
         bridge = new JMSBridgeImpl(cff0, cff0, sourceQueueFactory, localTargetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  batchSize, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

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
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, 0, NUM_MESSAGES, false);
                         
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
   
      
   private void testNoMaxBatchTime(QualityOfServiceMode qosMode, boolean persistent) throws Exception
   {
      JMSBridgeImpl bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  NUM_MESSAGES, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
            
         //Send half the messges

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, false);
                         
         //Verify none are received
         
         this.checkEmpty(targetQueue, 1);
         
         //Send the other half
         
         this.sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent, false);
         
         //This should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, false);
         
         //Send another batch with one more than batch size
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES + 1, persistent, false);
                  
         //Make sure only batch size are received
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, false);
         
         //Final batch
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES - 1, persistent, false);
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, NUM_MESSAGES, 1, false );
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES - 1, false);
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
   
   private void testNoMaxBatchTimeSameServer(QualityOfServiceMode qosMode, boolean persistent) throws Exception
   {
      JMSBridgeImpl bridge = null;
            
      try
      {
         final int NUM_MESSAGES = 10;
         
         bridge = new JMSBridgeImpl(cff0, cff0, sourceQueueFactory, localTargetQueueFactory,
                  null, null, null, null,
                  null, 5000, 10, qosMode,
                  NUM_MESSAGES, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
            
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, false);
         
         this.checkEmpty(targetQueue, 1);                
         
         //Send the other half
         
         this.sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES /2, persistent, false);
         
         
         //This should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, 0, NUM_MESSAGES, false);
         
         this.checkEmpty(localTargetQueue, 0);
         
         this.checkEmpty(sourceQueue, 0);
         
         //Send another batch with one more than batch size
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES + 1, persistent, false);
         
         //Make sure only batch size are received
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, 0, NUM_MESSAGES, false);
         
         //Final batch
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES - 1, persistent, false);
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, NUM_MESSAGES, 1, false);
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, 0, NUM_MESSAGES - 1, false);
      }
      finally
      {               
         if (bridge != null)
         {
            bridge.stop();
         }                  
      }                  
   }
   
   private void testMaxBatchTime(QualityOfServiceMode qosMode, boolean persistent) throws Exception
   {
      JMSBridgeImpl bridge = null;
            
      try
      {
         final long MAX_BATCH_TIME = 3000;
         
         final int MAX_BATCH_SIZE = 100000; // something big so it won't reach it
         
         bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 3000, 10, qosMode,
                  MAX_BATCH_SIZE, MAX_BATCH_TIME,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
            
         final int NUM_MESSAGES = 10;
         
         //Send some message

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES, persistent, false);                 
         
         //Verify none are received
         
         this.checkEmpty(targetQueue, 1);
         
         //Messages should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf1, targetQueue, 0, NUM_MESSAGES, false);         
      }
      finally
      {      
         if (bridge != null)
         {
            bridge.stop();
         }         
      }                  
   }
   
   private void testMaxBatchTimeSameServer(QualityOfServiceMode qosMode, boolean persistent) throws Exception
   {
      JMSBridgeImpl bridge = null;
            
      try
      {
         final long MAX_BATCH_TIME = 3000;
         
         final int MAX_BATCH_SIZE = 100000; // something big so it won't reach it
         
         bridge = new JMSBridgeImpl(cff0, cff0, sourceQueueFactory, localTargetQueueFactory,
                  null, null, null, null,
                  null, 3000, 10, qosMode,
                  MAX_BATCH_SIZE, MAX_BATCH_TIME,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
         
         final int NUM_MESSAGES = 10;
         
         //Send some message

         //Send some message

         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES, persistent, false);                 
         
         //Verify none are received
         
         this.checkEmpty(localTargetQueue, 0);;
         
         //Messages should now be receivable
         
         this.checkAllMessageReceivedInOrder(cf0, localTargetQueue, 0, NUM_MESSAGES, false);
      }
      finally
      {              
         if (bridge != null)
         {
            bridge.stop();
         }        
      }                  
   }

   public void testSetTMClass() throws Exception
   {
      JMSBridgeImpl bridge = null;

      try
      {
         bridge = new JMSBridgeImpl(cff0, cff0, sourceQueueFactory, localTargetQueueFactory,
                  null, null, null, null,
                  null, 3000, 10, QualityOfServiceMode.AT_MOST_ONCE,
                  10000, 3000,
                  null, null, false);
         bridge.setTransactionManagerLocatorClass(this.getClass().getName());
         bridge.setTransactionManagerLocatorMethod("getNewTm");
         bridge.start();
      }
      finally
      {
         if (bridge != null)
         {
            bridge.stop();
         }
      }
   }

   public TransactionManager getNewTm()
   {
      return newTransactionManager();
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
