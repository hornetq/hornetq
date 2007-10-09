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
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A test for distributed request-response pattern
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2701 $</tt>
 *
 * $Id: TemporaryDestinationTest.java 2701 2007-05-17 16:01:05Z timfox $
 */
public class DistributedRequestResponseTest extends ClusteringTestBase
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
   
   // http://jira.jboss.com/jira/browse/JBMESSAGING-1024
   public void testSuckAfterKill() throws Exception
   {
   	ServerManagement.kill(2);
   	
   	Thread.sleep(3000);
   	
   	distributedRequestResponse(false, true);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
   	nodeCount = 3;
   	
      super.setUp();     
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();    	
   }

   // Private --------------------------------------------------------------------------------------


   private void distributedRequestResponse(boolean tempQueue, final boolean persistent) throws Exception
   {
   	Connection conn0 = null;   	
   	Connection conn1 = null;
   	
      try
      {      	
         conn0 = this.createConnectionOnServer(cf, 0);
         
         conn1 = this.createConnectionOnServer(cf, 1);
         
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
						log.info("Received message in listener");
						Destination dest = msg.getJMSReplyTo();
						MessageProducer prod = sess.createProducer(dest);
						prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
						TextMessage tm = (TextMessage)msg;
						String text = tm.getText();
						tm.clearBody();
						tm.setText(text + "reply");
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
         
         MessageConsumer cons1 = session1.createConsumer(queue[1]);
         
         MyListener listener = new MyListener(session1);
         
         cons1.setMessageListener(listener);
         
         conn1.start();
         
                                    
         MessageProducer prod = session0.createProducer(queue[0]);
         
         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         
         for (int i = 0; i < 20; i++)
         {
         	TextMessage sm = session0.createTextMessage("hoo ja ma flip" + i);
            
            sm.setJMSReplyTo(tempDest);
            
            prod.send(sm);
            
            log.info("Sent message");
            
            TextMessage tm = (TextMessage)cons0.receive(60000);
            
            assertNotNull(tm);
            
            assertEquals(sm.getText() + "reply", tm.getText());
            
            log.info("Got reply");
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
         
         Thread.sleep(4000);
      }
   }
   
   // Inner classes --------------------------------------------------------------------------------

}
