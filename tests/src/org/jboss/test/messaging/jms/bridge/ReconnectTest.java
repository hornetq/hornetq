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
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;

/**
 * 
 * A ReconnectTest
 *
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

   protected void setUp() throws Exception
   {   
      if (!ServerManagement.isRemote())
      {
         fail("Test should only be run in a remote configuration");
      }
      
      useArjuna = true;
      
      super.setUp();                
   }

   protected void tearDown() throws Exception
   {      
      super.tearDown();      
   }
      
   // Crash and reconnect
   
   //Once and only once
   
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   //dups ok
   
   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception
   {
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   //At most once
   
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
   
   
   
   // Note this test will fail until http://jira.jboss.com/jira/browse/JBTM-192 is complete
   public void x_testCrashAndReconnectDestCrashOnCommit_P() throws Exception
   {
      testCrashAndReconnectDestCrashOnCommit(true);
   }
   
   // Note this test will fail until http://jira.jboss.com/jira/browse/JBTM-192 is complete
   public void x_testCrashAndReconnectDestCrashOnCommit_NP() throws Exception
   {
      testCrashAndReconnectDestCrashOnCommit(false);
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
         setUpAdministeredObjects();
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, qosMode,
                  10, -1,
                  null, null);
         
         bridge.start();
            
         final int NUM_MESSAGES = 10;
         
         //Send some messages
         
         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES /2 , persistent);
         
         //Verify none are received
         
         checkNoneReceived(cf1, destQueue);
         
         //Now crash the dest server
         
         log.info("About to crash server");
         
         ServerManagement.kill(1);
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 15 secs before bringing server back up");
         Thread.sleep(15000);
         log.info("Done wait");
         
         //Restart the server
         
         log.info("Restarting server");
         
         ServerManagement.start(1, "all", false);
         
         ServerManagement.deployQueue("destQueue", 1);
                                    
         setUpAdministeredObjects();
         
         //Send some more messages
         
         log.info("Sending more messages");
         
         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
         
         Thread.sleep(3000);
                  
         checkMessagesReceived(cf1, destQueue, qosMode, NUM_MESSAGES);
                    
         //Make sure no messages are left in the source dest
         
         this.checkNoneReceived(cf0, sourceQueue);                
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
         setUpAdministeredObjects();
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
                  10, 5000,
                  null, null);
         
         bridge.start();
         
         final int NUM_MESSAGES = 10;
            
         //Send some messages
         
         this.sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
         
         
         //verify none are received
         
         this.checkNoneReceived(cf1, destQueue);
         
         
         //Now crash the dest server
         
         log.info("About to crash server");
         
         ServerManagement.kill(1);
         
         //Wait a while before starting up to simulate the dest being down for a while
         log.info("Waiting 15 secs before bringing server back up");
         Thread.sleep(15000);
         log.info("Done wait");
         
         //Restart the server
         
         ServerManagement.start(1, "all", false);
         
         ServerManagement.deployQueue("destQueue", 1);
                           
         setUpAdministeredObjects();
         
         sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
                           
         checkMessagesReceived(cf1, destQueue, Bridge.QOS_ONCE_AND_ONLY_ONCE, NUM_MESSAGES);
         
         //Make sure no messages are left in the source dest
         
         checkNoneReceived(cf0, sourceQueue);
         
         log.info("Got here");
         
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
    * Crash the server after prepare but on commit
    * Bring up the destination server
    * Send some more messages
    * Verify all messages are received
    */
   private void testCrashAndReconnectDestCrashOnCommit(boolean persistent) throws Exception
   {
      Bridge bridge = null;
            
      try
      {
         setUpAdministeredObjects();
         
         final int NUM_MESSAGES = 10;         
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
                  NUM_MESSAGES, 5000,
                  null, null);
         
         bridge.start();
         
         //Send some messages
         
         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent);
         

         //Verify none are received
         
         checkNoneReceived(cf1, destQueue);
         

         //Poison server 1 so it crashes on commit of dest but after prepare
         
         //This means the transaction branch on source will get commmitted
         //but the branch on dest won't be - it will remain prepared
         //This corresponds to a HeuristicMixedException
         
         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);
         
         ServerManagement.nullServer(1);
         
         log.info("Poisoned server");
         
                     
         //Wait for maxBatchTime to kick in so a batch is sent
         //This should cause the server to crash after prepare but before commit
         
         //Also the wait must be enough to allow transaction recovery to kick in
         //Since there will be a heuristically prepared branch on the consumer that needs to be rolled
         //back
         
         Thread.sleep(10000);
               
         //Restart the server
         
         log.info("Restarting server");
                  
         ServerManagement.start(1, "all", false);
         
         log.info("Restarted server");
         
         ServerManagement.deployQueue("destQueue", 1);
         
         //Give enough time for transaction recovery to happen
         Thread.sleep(20000);
         
         log.info("Deployed queue");
         
         log.info("Slept");
                           
         setUpAdministeredObjects();
         
           
         //Send some more messages
         
         this.sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent);
                  
         checkMessagesReceived(cf1, destQueue, Bridge.QOS_ONCE_AND_ONLY_ONCE, NUM_MESSAGES);
         
         //Make sure no messages are left in the source dest
         
         this.checkNoneReceived(cf0, sourceQueue);
         
         log.info("Got here");
         
      }
      finally
      {      
         log.info("In finally");         
         
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
