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
package org.jboss.test.messaging.jms.clustering;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A test for distributed request-response pattern
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2701 $</tt>
 *
 * $Id: TemporaryDestinationTest.java 2701 2007-05-17 16:01:05Z timfox $
 */
public class DistributedRequestResponseTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DistributedRequestResponseTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------
      
   public void testDistributedRequestResponseWithTempTopicP() throws Exception
   {
   	distributedRequestResponse(false, true);
   } 
   
   public void testDistributedRequestResponseWithTempTopicNP() throws Exception
   {
   	distributedRequestResponse(false, false);
   } 
   
   public void testDistributedRequestResponseWithTempQueueP() throws Exception
   {
   	distributedRequestResponse(true, true);
   } 
   
   public void testDistributedRequestResponseWithTempQueueNP() throws Exception
   {
   	distributedRequestResponse(true, false);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start(0, "all", null, true);
      ServerManagement.start(1, "all", null, false);

      ServerManagement.deployQueue("testDistributedQueue", 0);
      ServerManagement.deployQueue("testDistributedQueue", 1);
      
      removeAllMessages("testDistributedQueue", true, 0);
      removeAllMessages("testDistributedQueue", true, 1);

      

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
   	ServerManagement.undeployQueue("testDistributedQueue", 0);
   	
      ServerManagement.undeployQueue("testDistributedQueue", 1);         	
   }

   // Private --------------------------------------------------------------------------------------


   private void distributedRequestResponse(boolean tempQueue, final boolean persistent) throws Exception
   {
   	Connection conn0 = null;   	
   	Connection conn1 = null;
      
      try
      {      
	      InitialContext ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));
	      InitialContext ic1 = new InitialContext(ServerManagement.getJNDIEnvironment(1));
	
	      ConnectionFactory cf = (ConnectionFactory)ic0.lookup("/ClusteredConnectionFactory");
	      
	      Queue queue0 = (Queue)ic0.lookup("/queue/testDistributedQueue");
	      Queue queue1 = (Queue)ic1.lookup("/queue/testDistributedQueue");
	   	
         conn0 = this.createConnectionOnServer(cf, 0);
         
         conn1 = cf.createConnection();
         
         assertEquals(0, getServerId(conn0));
         
         assertEquals(1, getServerId(conn1));
         
         // Make sure the connections are on different servers
         
         Session session0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Destination tempDest;
         
         if (tempQueue)
         {
         	tempDest = session0.createTemporaryQueue();
         }
         else
         {
         	tempDest = session0.createTemporaryTopic();
         }
         
         MessageConsumer cons0 = session0.createConsumer(tempDest);
         
         Thread.sleep(2000);
         
         conn0.start();
          
         class MyListener implements MessageListener
         {
         	Session sess;
         	
         	MyListener(Session sess)
         	{
         		this.sess = sess;
         	}

				public void onMessage(Message msg)
				{
					try
					{
						log.info("Received message in listener!");
						Destination dest = msg.getJMSReplyTo();
						MessageProducer prod = sess.createProducer(dest);
						prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
						TextMessage tm = (TextMessage)msg;
						String text = tm.getText();
						tm.clearBody();
						tm.setText(text + "reply");
						log.info("Sending response");
						prod.send(msg);
					}
					catch (JMSException e)
					{
						log.error("Failed to reply to message", e);
                  fail();
					}
				}
         	
         }
         
                  
         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons1 = session1.createConsumer(queue1);
         
         MyListener listener = new MyListener(session1);
         
         cons1.setMessageListener(listener);
         
         conn1.start();
         
                                    
         MessageProducer prod = session0.createProducer(queue0);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < 20; i++)
         {
         	TextMessage sm = session0.createTextMessage("hoo ja ma flip" + i);
            
            sm.setJMSReplyTo(tempDest);
            
            log.info("Sending message!");
            
            prod.send(sm);
            
            TextMessage tm = (TextMessage)cons0.receive(60000);
            
            assertNotNull(tm);
            
            assertEquals(sm.getText() + "reply", tm.getText());
            
            log.info("Received reply!");
         }   
      }
      finally
      {

         if (conn0 != null)
         {
            conn0.close();
         }
         
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }
   
   private void removeAllMessages(String destName, boolean isQueue, int server) throws Exception
   {
   	String on = "jboss.messaging.destination:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + destName;
   	
   	ServerManagement.getServer(server).invoke(new ObjectName(on), "removeAllMessages", null, null);
   }
   
   // Inner classes --------------------------------------------------------------------------------

}
