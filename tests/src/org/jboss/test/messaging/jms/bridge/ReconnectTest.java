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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

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
import org.jboss.test.messaging.tools.ServerManagement;

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
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, true);
   }
   
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_ONCE_AND_ONLY_ONCE, false);
   }
   
   //dups ok
   
   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, true);
   }
   
   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_DUPLICATES_OK, false);
   }
   
   //At most once
   
   public void testCrashAndReconnectDestBasic_AtMostOnce_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_AT_MOST_ONCE, true);
   }
   
   public void testCrashAndReconnectDestBasic_AtMostOnce_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestBasic(Bridge.QOS_AT_MOST_ONCE, false);
   }
   

   // Crash tests specific to XA transactions
   
   public void testCrashAndReconnectDestCrashBeforePrepare_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestCrashBeforePrepare(true);
   }
   
   public void testCrashAndReconnectDestCrashBeforePrepare_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestCrashBeforePrepare(false);
   }
   
   
   
   public void testCrashAndReconnectDestCrashOnCommit_P() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      testCrashAndReconnectDestCrashOnCommit(true);
   }
   
   public void testCrashAndReconnectDestCrashOnCommit_NP() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
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
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
         
      try
      {
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
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, qosMode,
                  10, -1,
                  null, null);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);                          
         
         final int NUM_MESSAGES = 10;
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         //Verify none are received
         
         Message m = cons.receive(1000);
         
         assertNull(m);
         
         connDest.close();
         
         ic1.close();
         
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
                           
         cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
               
         ic1 = new InitialContext(props1);
         
         cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
                  
         destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         connDest = cf1.createConnection();
         
         sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
           
         //Send some more messages
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
                  
         //If Qos=once and only once then all messages should be received
         //If Qos=at most then only the second half will be received
         //If Qos=dups ok then the the first half will be received twice followed by the second half
         
         checkMessagesReceived(qosMode, cons, NUM_MESSAGES);
                    
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
            try
            {
               bridge.stop();
            }
            catch (Exception e)
            {
               log.error("Failed to stop bridge", e);
            }
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
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
            
      try
      {
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
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
                  10, 5000,
                  null, null);
         
         bridge.start();
            
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);                          
         
         final int NUM_MESSAGES = 10;
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         //Verify none are received
         
         Message m = cons.receive(1000);
         
         assertNull(m);
         
         connDest.close();
         
         ic1.close();
         
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
                           
         cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
               
         ic1 = new InitialContext(props1);
         
         cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
                  
         destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         connDest = cf1.createConnection();
         
         sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
           
         //Send some more messages
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
                  
         checkMessagesReceived(Bridge.QOS_ONCE_AND_ONLY_ONCE, cons, NUM_MESSAGES);
         
         //Make sure no messages are left in the source dest
         
         MessageConsumer cons2 = sessSend.createConsumer(sourceQueue);
         
         connSource.start();
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         log.info("Got here");
         
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
            try
            {
               bridge.stop();
            }
            catch (Exception e)
            {
               log.error("Failed to stop bridge", e);
            }
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
      Connection connSource = null;
      
      Connection connDest = null;
      
      Bridge bridge = null;
            
      try
      {
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
         
         final int NUM_MESSAGES = 10;         
         
         bridge = new Bridge(cff0, cff1, sourceQueue, destQueue,
                  null, null, null, null,
                  null, 1000, -1, Bridge.QOS_ONCE_AND_ONLY_ONCE,
                  NUM_MESSAGES, 5000,
                  null, null);
         
         bridge.start();
         
         connSource = cf0.createConnection();
         
         connDest = cf1.createConnection();
         
         Session sessSend = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(sourceQueue);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);                          
         
         for (int i = 0; i < NUM_MESSAGES / 2; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         Session sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
         
         //Verify none are received
         
         Message m = cons.receive(1000);
          
         assertNull(m);
         
         connDest.close();
         
         ic1.close();
         
         //Poison server 1 so it crashes on commit of dest but after prepare
         
         //This means the transaction branch on source will get commmitted
         //but the branch on dest won't be - it will remain prepared
         //This corresponds to a HeuristicMixedException
         
         ServerManagement.poisonTheServer(1);
         
         ServerManagement.nullServer(1);
         
         log.info("Poisoned server");
         
                     
         //Wait for maxBatchTime to kick in so a batch is sent
         //This should cause the server to crash after prepare but before commit
         
         //Also the wait must be enough to allow transaction recovery to kick in
         //Since there will be a heuristically prepared branch on the consumer that needs to be rolled
         //back
         
         Thread.sleep(20000);
               
         //Restart the server
         
         log.info("Restarting server");
                  
         ServerManagement.start(1, "all", false);
         
         log.info("Restarted server");
         
         ServerManagement.deployQueue("destQueue", 1);
         
         log.info("Deployed queue");
         
         log.info("Slept");
                           
         cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
               
         ic1 = new InitialContext(props1);
         
         cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
                  
         destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         connDest = cf1.createConnection();
         
         sessRec = connDest.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         cons = sessRec.createConsumer(destQueue);
         
         connDest.start();
           
         //Send some more messages
         
         for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);
            
            prod.send(tm);
         }
                  
         checkMessagesReceived(Bridge.QOS_ONCE_AND_ONLY_ONCE, cons, NUM_MESSAGES);
         
         //Make sure no messages are left in the source dest
         
         MessageConsumer cons2 = sessSend.createConsumer(sourceQueue);
         
         connSource.start();
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
         log.info("Got here");
         
      }
      finally
      {      
         log.info("In finally");
         
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
            try
            {
               bridge.stop();
            }
            catch (Exception e)
            {
               log.error("Failed to stop bridge", e);
            }
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
      }                  
   }
   
   private void checkMessagesReceived(int qosMode, MessageConsumer cons, int numMessages) throws Exception
   {
      //Consume the messages
      
      Set msgs = new HashSet();
      
      while (true)
      {
         TextMessage tm = (TextMessage)cons.receive(2000);
         
         if (tm == null)
         {
            break;
         }
         
         msgs.add(tm.getText());
       
      }
      
      if (qosMode == Bridge.QOS_ONCE_AND_ONLY_ONCE || qosMode == Bridge.QOS_DUPLICATES_OK)
      {            
         //All the messages should be received
         
         for (int i = 0; i < numMessages; i++)
         {
            assertTrue(msgs.contains("message" + i));
         }
         
         //Should be no more
         if (qosMode == Bridge.QOS_ONCE_AND_ONLY_ONCE)
         {
            assertEquals(numMessages, msgs.size());
         }         
      }
      else if (qosMode == Bridge.QOS_AT_MOST_ONCE)
      {
         //No *guarantee* that any messages will be received
         //but you still might get some depending on how/where the crash occurred                 
      }            
   }
   
   // Inner classes -------------------------------------------------------------------
   
}
