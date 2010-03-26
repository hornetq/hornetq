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

import junit.framework.Assert;

import org.hornetq.core.logging.Logger;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.bridge.impl.JMSBridgeImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JMSBridgeReconnectionTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(JMSBridgeReconnectionTest.class);

   // Crash and reconnect

   // Once and only once

   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, false);
   }

   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P_LargeMessage() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, true);
   }

   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, false);
   }

   // dups ok

   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, true, false);
   }

   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, false, false);
   }

   // At most once

   public void testCrashAndReconnectDestBasic_AtMostOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, true, false);
   }

   public void testCrashAndReconnectDestBasic_AtMostOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, false, false);
   }

   // Crash tests specific to XA transactions

   public void testCrashAndReconnectDestCrashBeforePrepare_P() throws Exception
   {
      testCrashAndReconnectDestCrashBeforePrepare(true);
   }

   public void testCrashAndReconnectDestCrashBeforePrepare_NP() throws Exception
   {
      testCrashAndReconnectDestCrashBeforePrepare(false);
   }

   // Crash before bridge is started

   public void testRetryConnectionOnStartup() throws Exception
   {
      jmsServer1.stop();

      JMSBridgeImpl bridge = new JMSBridgeImpl(cff0,
                                               cff1,
                                               sourceQueueFactory,
                                               targetQueueFactory,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               1000,
                                               -1,
                                               QualityOfServiceMode.DUPLICATES_OK,
                                               10,
                                               -1,
                                               null,
                                               null,
                                               false);
      bridge.setTransactionManager(newTransactionManager());

      try
      {
         bridge.start();
         Assert.assertFalse(bridge.isStarted());
         Assert.assertTrue(bridge.isFailed());

         // Restart the server
         jmsServer1.start();

         createQueue("targetQueue", 1);
         setUpAdministeredObjects();

         Thread.sleep(3000);

         Assert.assertTrue(bridge.isStarted());
         Assert.assertFalse(bridge.isFailed());
      }
      finally
      {
         try
         {
            bridge.stop();
         }
         catch (Exception e)
         {
            JMSBridgeReconnectionTest.log.error("Failed to stop bridge", e);
         }
      }
   }

   /*
    * Send some messages
    * Crash the destination server
    * Bring the destination server back up
    * Send some more messages
    * Verify all messages are received
    */
   private void testCrashAndReconnectDestBasic(final QualityOfServiceMode qosMode,
                                               final boolean persistent,
                                               final boolean largeMessage) throws Exception
   {
      JMSBridgeImpl bridge = null;

      try
      {
         bridge = new JMSBridgeImpl(cff0,
                                    cff1,
                                    sourceQueueFactory,
                                    targetQueueFactory,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    1000,
                                    -1,
                                    qosMode,
                                    10,
                                    -1,
                                    null,
                                    null,
                                    false);
         bridge.setTransactionManager(newTransactionManager());
         bridge.start();

         final int NUM_MESSAGES = 10;

         // Send some messages

         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, largeMessage);

         // Verify none are received

         checkEmpty(targetQueue, 1);

         // Now crash the dest server

         JMSBridgeReconnectionTest.log.info("About to crash server");

         jmsServer1.stop();

         // Wait a while before starting up to simulate the dest being down for a while
         JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(10000);
         JMSBridgeReconnectionTest.log.info("Done wait");

         // Restart the server
         JMSBridgeReconnectionTest.log.info("Restarting server");
         jmsServer1.start();

         jmsServer1.createQueue("targetQueue", null, true, "queue/targetQueue");

         createQueue("targetQueue", 1);

         setUpAdministeredObjects();

         // Send some more messages

         JMSBridgeReconnectionTest.log.info("Sending more messages");

         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent, largeMessage);

         JMSBridgeReconnectionTest.log.info("Sent messages");

         checkMessagesReceived(cf1, targetQueue, qosMode, NUM_MESSAGES, false, largeMessage);
      }
      finally
      {

         if (bridge != null)
         {
            try
            {
               bridge.stop();
            }
            catch (Exception e)
            {
               JMSBridgeReconnectionTest.log.error("Failed to stop bridge", e);
            }
         }
      }
   }

   /*
    * Send some messages
    * Crash the destination server
    * Set the max batch time such that it will attempt to send the batch while the dest server is down
    * Bring up the destination server
    * Send some more messages
    * Verify all messages are received
    */
   private void testCrashAndReconnectDestCrashBeforePrepare(final boolean persistent) throws Exception
   {
      JMSBridgeImpl bridge = null;

      try
      {
         bridge = new JMSBridgeImpl(cff0,
                                    cff1,
                                    sourceQueueFactory,
                                    targetQueueFactory,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    1000,
                                    -1,
                                    QualityOfServiceMode.ONCE_AND_ONLY_ONCE,
                                    10,
                                    5000,
                                    null,
                                    null,
                                    false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();

         final int NUM_MESSAGES = 10;
         // Send some messages

         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, false);

         // verify none are received

         checkEmpty(targetQueue, 1);

         // Now crash the dest server

         JMSBridgeReconnectionTest.log.info("About to crash server");

         jmsServer1.stop();

         // Wait a while before starting up to simulate the dest being down for a while
         JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(10000);
         JMSBridgeReconnectionTest.log.info("Done wait");

         // Restart the server
         jmsServer1.start();

         createQueue("targetQueue", 1);

         setUpAdministeredObjects();

         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent, false);

         checkMessagesReceived(cf1, targetQueue, QualityOfServiceMode.ONCE_AND_ONLY_ONCE, NUM_MESSAGES, false, false);
      }
      finally
      {

         if (bridge != null)
         {
            try
            {
               bridge.stop();
            }
            catch (Exception e)
            {
               JMSBridgeReconnectionTest.log.error("Failed to stop bridge", e);
            }
         }

      }
   }

   // Inner classes -------------------------------------------------------------------

}
