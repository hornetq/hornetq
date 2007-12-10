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

import org.jboss.jms.server.bridge.Bridge;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ReconnectTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(ReconnectTest.class);

   public ReconnectTest(String name)
   {
      super(name);
   }
        
   // Crash and reconnect
   
   // Once and only once
   
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }

   // dups ok

   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, true);
   }

   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, false);
   }

   // At most once

   public void testCrashAndReconnectDestBasic_AtMostOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_AT_MOST_ONCE, true);
   }

   public void testCrashAndReconnectDestBasic_AtMostOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_AT_MOST_ONCE, false);
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
      ServerManagement.kill(1);

      Bridge bridge = new Bridge(cff0, cff1, sourceQueueFactory, targetQueueFactory,
            null, null, null, null,
            null, 1000, -1, Bridge.QOS_DUPLICATES_OK,
            10, -1,
            null, null, false);
      
      try
      {
         bridge.start();
         assertFalse(bridge.isStarted());
         assertTrue(bridge.isFailed());

         ServerManagement.start(1, "all", false);
         deployQueue("targetQueue", 1);
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
   private void testCrashAndReconnectDestBasic(int qosMode, boolean persistent) throws Exception
   {
      Bridge bridge = null;
         
      try
      {   
         bridge = new Bridge(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 1000, -1, qosMode,
                  10, -1,
                  null, null, false);
         
         bridge.start();
            
         final int NUM_MESSAGES = 10;
         
         //Send some messages
         
         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2 , persistent);
         
         //Verify none are received
         
         checkEmpty(targetQueue, 1);
         
         //Now crash the dest server
         
         log.info("About to crash server");
         
         ServerManagement.kill(1);
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(5000);
         log.info("Done wait");
         
         //Restart the server
         
         log.info("Restarting server");
         
         ServerManagement.start(1, "all", false);
         
         deployQueue("targetQueue", 1);
         
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
      Bridge bridge = null;
            
      try
      {
         bridge = new Bridge(cff0, cff1, sourceQueueFactory, targetQueueFactory,
                  null, null, null, null,
                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
                  10, 5000,
                  null, null, false);
         
         bridge.start();
         
         final int NUM_MESSAGES = 10;            
         //Send some messages
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
                  
         //verify none are received
         
         checkEmpty(targetQueue, 1);
                  
         //Now crash the dest server
         
         log.info("About to crash server");
         
         ServerManagement.kill(1);
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 5 secs before bringing server back up");
         Thread.sleep(5000);
         log.info("Done wait");
         
         //Restart the server
         
         ServerManagement.start(1, "all", false);
         
         deployQueue("targetQueue", 1);
         
         setUpAdministeredObjects();
         
         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
                           
         checkMessagesReceived(cf1, targetQueue, Bridge.QOS_ONCE_AND_ONLY_ONCE, NUM_MESSAGES, false);         
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
