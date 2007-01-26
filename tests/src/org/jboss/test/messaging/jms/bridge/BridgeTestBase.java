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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.jms.server.bridge.Bridge;
import org.jboss.jms.server.bridge.ConnectionFactoryFactory;
import org.jboss.jms.server.bridge.JNDIConnectionFactoryFactory;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * 
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeTestBase extends MessagingTestCase
{
   private static final Logger log = Logger.getLogger(BridgeTestBase.class);
   
   protected int nodeCount = 2;
   
   protected ServiceContainer sc;
   
   protected ConnectionFactoryFactory cff0, cff1;
   
   protected ConnectionFactory cf0, cf1;
   
   protected Destination sourceQueue, destQueue, sourceTopic, localDestQueue;
   
   protected boolean useArjuna;
   
   public BridgeTestBase(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
       
      log.info("Starting " + nodeCount + " servers");
      
      if (ServerManagement.isRemote())
      {                 
         for (int i = 0; i < nodeCount; i++)
         {
            // make sure all servers are created and started; make sure that database is zapped
            // ONLY for the first server, the others rely on values they expect to find in shared
            // tables; don't clear the database for those.
            if (useArjuna)
            {
               ServerManagement.start(i, "all,-transaction, jbossjta", i == 0);
            }
            else
            {
               ServerManagement.start(i, "all", i == 0);
            }
         }
         
         //We need a local transaction and recovery manager
         //We must start this after the remote servers have been created or it won't
         //have deleted the database and the recovery manager may attempt to recover transactions
         if (useArjuna)
         {
            sc = new ServiceContainer("jbossjta");            
         }
         else
         {
            sc = new ServiceContainer("transaction");
         }
         sc.start(false);
         
         ServerManagement.undeployQueue("sourceQueue", 0);
         
         ServerManagement.undeployTopic("sourceTopic", 0);  
         
         ServerManagement.undeployQueue("localDestQueue", 0);
            
         ServerManagement.undeployQueue("destQueue", 1); 
         
         ServerManagement.deployQueue("sourceQueue", 0);
         
         ServerManagement.deployTopic("sourceTopic", 0);  
         
         ServerManagement.deployQueue("localDestQueue", 0);
            
         ServerManagement.deployQueue("destQueue", 1);         
      }                  
   }

   protected void tearDown() throws Exception
   { 
      if (ServerManagement.isRemote())
      {         
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
            ServerManagement.undeployTopic("sourceTopic", 0);
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
         
         try
         {
            ServerManagement.undeployQueue("localDestQueue", 0);
         }
         catch (Exception e)
         {
            log.error("Failed to undeploy", e);
         }
         
         
         
         for (int i = 0; i < nodeCount; i++)
         {
            try
            {
               if (ServerManagement.isStarted(i))
               {
                  ServerManagement.log(ServerManagement.INFO, "Undeploying Server " + i, i);
                  
                  ServerManagement.stop(i);
               }
            }
            catch (Exception e)
            {
               log.error("Failed to stop server", e);
            }
         }
         
         for (int i = 1; i < nodeCount; i++)
         {
            try
            {
               ServerManagement.kill(i);
            }
            catch (Exception e)
            {
               log.error("Failed to kill server", e);
            }
         }
         
         sc.stop();
      }
      
      
      
      super.tearDown();
      
   }
   
   protected void setUpAdministeredObjects() throws Exception
   {
      InitialContext ic0 = null, ic1 = null;
      try
      {
         Hashtable props0 = ServerManagement.getJNDIEnvironment(0);
         
         Hashtable props1 = ServerManagement.getJNDIEnvironment(1);
         
         cff0 = new JNDIConnectionFactoryFactory(props0, "/ConnectionFactory");
         
         cff1 = new JNDIConnectionFactoryFactory(props1, "/ConnectionFactory");
               
         ic0 = new InitialContext(props0);
         
         ic1 = new InitialContext(props1);
         
         cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");
         
         cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
         
         sourceQueue = (Queue)ic0.lookup("/queue/sourceQueue");
         
         destQueue = (Queue)ic1.lookup("/queue/destQueue");
         
         sourceTopic = (Topic)ic0.lookup("/topic/sourceTopic");
         
         localDestQueue = (Queue)ic0.lookup("/queue/localDestQueue");
      }
      finally
      {
         if (ic0 != null)
         {
            ic0.close();
         }
         if (ic1 != null)
         {
            ic1.close();
         }
      }    
   }
   
   protected void sendMessages(ConnectionFactory cf, Destination dest, int start, int numMessages, boolean persistent)
      throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(dest);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = start; i < start + numMessages; i++)
         {
            TextMessage tm = sess.createTextMessage("message" + i);
            
            prod.send(tm);
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   protected void checkNoneReceived(ConnectionFactory cf, Destination dest) throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createConsumer(dest);
         
         Message m = cons.receive(2000);
         
         assertNull(m);
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   protected void checkMessagesReceived(ConnectionFactory cf, Destination dest, int qosMode, int numMessages) throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createConsumer(dest);
         
         // Consume the messages
         
         Set msgs = new HashSet();
         
         int count = 0;
         
         while (true)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            if (tm == null)
            {
               break;
            }
            
            msgs.add(tm.getText());
            
            count++;
            
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
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }  
   }
   
   
   protected void checkAllMessageReceivedInOrder(ConnectionFactory cf, Destination dest, int start, int numMessages) throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sess.createConsumer(dest);
         
         // Consume the messages
           
         for (int i = 0; i < numMessages; i++)
         {            
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);
            
            assertEquals("message" + (i + start), tm.getText());
         } 
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }  
   }
   
   
   // Inner classes -------------------------------------------------------------------
   
}

