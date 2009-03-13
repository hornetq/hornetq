/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.integration.jms.bridge;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.bridge.QualityOfServiceMode;
import org.jboss.messaging.jms.bridge.impl.BridgeImpl;
// import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.unit.util.InVMContext;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeReconnectionTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(BridgeReconnectionTest.class);

   // Crash and reconnect
   
   // Once and only once
   
   public void _testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true);
   }
   
   public void _testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false);
   }

   // dups ok

   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, true);
   }

   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, false);
   }

   // At most once

   public void testCrashAndReconnectDestBasic_AtMostOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, true);
   }

   public void testCrashAndReconnectDestBasic_AtMostOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, false);
   }

   // Crash tests specific to XA transactions

   public void _testCrashAndReconnectDestCrashBeforePrepare_P() throws Exception
   {
      testCrashAndReconnectDestCrashBeforePrepare(true);
   }

   public void _testCrashAndReconnectDestCrashBeforePrepare_NP() throws Exception
   {
      testCrashAndReconnectDestCrashBeforePrepare(false);
   }
   
   // Crash before bridge is started

   public void testRetryConnectionOnStartup() throws Exception
   {
      server1.stop();

      BridgeImpl bridge = new BridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
            null, null, null, null,
            null, 1000, -1, QualityOfServiceMode.DUPLICATES_OK,
            10, -1,
            null, null, false);
      bridge.setTransactionManager(newTransactionManager());

      try
      {
         bridge.start();
         assertFalse(bridge.isStarted());
         assertTrue(bridge.isFailed());

         //Restart the server         
         server1.start();
         
         context1 = new InVMContext();
         jmsServer1 = JMSServerManagerImpl.newJMSServerManagerImpl(server1.getServer());
         jmsServer1.start();
         jmsServer1.setContext(context1);

         createQueue("targetQueue", 1);
         setUpAdministeredObjects();
         
         Thread.sleep(3000);
         
         assertTrue(bridge.isStarted());
         assertFalse(bridge.isFailed());
      }
      finally
      {
         try
         {
            bridge.stop();
         }
         catch (Exception e)
         {
            log.error("Failed to stop bridge", e);
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
   private void testCrashAndReconnectDestBasic(QualityOfServiceMode qosMode, boolean persistent) throws Exception
   {
      BridgeImpl bridge = null;
         
      try
      {   
         bridge = new BridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 1000, -1, qosMode,
                  10, -1,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());
         bridge.start();
            
         final int NUM_MESSAGES = 10;
         
         //Send some messages
         
         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2 , persistent);
         
         //Verify none are received
         
         checkEmpty(targetQueue, 1);
         
         //Now crash the dest server
         
         log.info("About to crash server");
         
         server1.stop();
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(5000);
         log.info("Done wait");
         
         //Restart the server
         
         log.info("Restarting server");
         
         server1.start();
         
         context1 = new InVMContext();
         jmsServer1 = JMSServerManagerImpl.newJMSServerManagerImpl(server1.getServer());
         jmsServer1.start();
         jmsServer1.setContext(context1);

         createQueue("targetQueue", 1);
         
         setUpAdministeredObjects();
         
         //Send some more messages
         
         log.info("Sending more messages");
         
         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
         
         log.info("Sent messages");
         
         checkMessagesReceived(cf1, targetQueue, qosMode, NUM_MESSAGES, false);                  
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
               log.error("Failed to stop bridge", e);
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
   private void testCrashAndReconnectDestCrashBeforePrepare(boolean persistent) throws Exception
   {   
      BridgeImpl bridge = null;
            
      try
      {
         bridge = new BridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 1000, -1, QualityOfServiceMode.ONCE_AND_ONLY_ONCE,
                  10, 5000,
                  null, null, false);
         bridge.setTransactionManager(newTransactionManager());

         bridge.start();
         
         final int NUM_MESSAGES = 10;            
         //Send some messages
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
                  
         //verify none are received
         
         checkEmpty(targetQueue, 1);
                  
         //Now crash the dest server
         
         log.info("About to crash server");
         
         server1.stop();
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(5000);
         log.info("Done wait");
         
         //Restart the server         
         server1.start();
         
         context1 = new InVMContext();
         jmsServer1 = JMSServerManagerImpl.newJMSServerManagerImpl(server1.getServer());
         jmsServer1.start();
         jmsServer1.setContext(context1);

         createQueue("targetQueue", 1);
         
         setUpAdministeredObjects();
         
         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
                           
         checkMessagesReceived(cf1, targetQueue, QualityOfServiceMode.ONCE_AND_ONLY_ONCE, NUM_MESSAGES, false);         
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
               log.error("Failed to stop bridge", e);
            }
         }
         
      }                  
   }
   
   // Inner classes -------------------------------------------------------------------
   
}
