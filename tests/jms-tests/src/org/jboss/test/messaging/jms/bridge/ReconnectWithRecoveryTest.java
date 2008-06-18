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
package org.jboss.test.messaging.jms.bridge;

import java.util.Properties;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ReconnectWithRecoveryTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(ReconnectTest.class);

   public ReconnectWithRecoveryTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {   
      super.setUp();         
      
      //Now install local JMSProviderAdaptor classes
      
      Properties props1 = new Properties();
      props1.putAll(ServerManagement.getJNDIEnvironment(1));
        
      /*JMSProviderAdapter targetAdaptor =
         new TestJMSProviderAdaptor(props1, "/XAConnectionFactory", "adaptor1");
      
      sc.installJMSProviderAdaptor("adaptor1", targetAdaptor);
      
      sc.startRecoveryManager();*/
   }

   protected void tearDown() throws Exception
   {  
      super.tearDown();

      /*sc.stopRecoveryManager();
      
      sc.uninstallJMSProviderAdaptor("adaptor1");*/

      log.debug(this + " torn down");
   }

   public void testNoop()
   {

   }
      
   /*
    * TODO: What to do with this test?
    * Send some messages   
    * Crash the server after prepare but on commit
    * Bring up the destination server
    * Send some more messages
    * Verify all messages are received
    */
//   public void testCrashAndReconnectDestCrashOnCommit() throws Exception
//   {
//      Bridge bridge = null;
//
//      try
//      {
//         final int NUM_MESSAGES = 10;
//
//         bridge = new Bridge(cff0, cff1, sourceQueueFactory, targetQueueFactory,
//                  null, null, null, null,
//                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
//                  NUM_MESSAGES, -1,
//                  null, null, false);
//
//         bridge.start();
//
//         //Poison server 1 so it crashes on commit of dest but after prepare
//
//         //This means the transaction branch on source will get commmitted
//         //but the branch on dest won't be - it will remain prepared
//         //This corresponds to a HeuristicMixedException
//
//         ServerManagement.poisonTheServer(1, PoisonInterceptor.TYPE_2PC_COMMIT);
//
//         log.info("Poisoned server");
//
//         //Send some messages
//
//         sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES, true);
//
//         //Restart the server
//
//         //Wait a bit for the batch to be sent - this should cause the server to crash on commit
//
//         Thread.sleep(3000);
//
//         log.info("Restarting server");
//
//         ServerManagement.start(1, "all", false);
//
//         log.info("Restarted server");
//
//         deployQueue("targetQueue", 1);
//
//         this.setUpAdministeredObjects();
//
//         log.info("Deployed queue");
//
//         log.info("*** waiting for recovery");
//
//         //There may be a long wait for the first time (need to let recovery kick in)
//         checkMessagesReceived(cf1, targetQueue, Bridge.QOS_ONCE_AND_ONLY_ONCE, NUM_MESSAGES, true);
//      }
//      finally
//      {
//         if (bridge != null)
//         {
//            try
//            {
//               bridge.stop();
//            }
//            catch (Exception e)
//            {
//               log.error("Failed to stop bridge", e);
//            }
//         }
//      }
//   }
   
   // Inner classes -------------------------------------------------------------------   
}

