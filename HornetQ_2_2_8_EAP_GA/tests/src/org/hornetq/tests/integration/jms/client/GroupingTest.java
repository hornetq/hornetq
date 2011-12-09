/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.tests.util.JMSTestBase;

/**
 * GroupingTest
 *
 * @author Tim Fox
 *
 */
public class GroupingTest extends JMSTestBase
{
   private static final Logger log = Logger.getLogger(GroupingTest.class);

   private Queue queue;

   protected void setUp() throws Exception
   {
      super.setUp();
      
      queue = createQueue("TestQueue");
   }
   
   protected void tearDown() throws Exception
   {
      jmsServer.destroyQueue("TestQueue");
      
      super.tearDown();
   }
   
   protected void setProperty(Message message)
   {
      ((HornetQMessage)message).getCoreMessage().putStringProperty(MessageImpl.HDR_GROUP_ID, new SimpleString("foo"));
   }
   
   protected ConnectionFactory getCF() throws Exception
   {
      return cf;
   }
   
   public void testGrouping() throws Exception
   {
      ConnectionFactory fact = getCF();
      
      Connection connection = fact.createConnection();
      
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      MessageProducer producer = session.createProducer(queue);
      
      MessageConsumer consumer1 = session.createConsumer(queue);
      MessageConsumer consumer2 = session.createConsumer(queue);
      MessageConsumer consumer3 = session.createConsumer(queue);
      
      connection.start();
      
      String jmsxgroupID = null;

      for (int j = 0; j < 100; j++)
      {
         TextMessage message = session.createTextMessage();
                  
         message.setText("Message" + j);
         
         setProperty(message);
         
         producer.send(message);
         
         String prop = message.getStringProperty("JMSXGroupID");
         
         assertNotNull(prop);
         
         if (jmsxgroupID != null)
         {
            assertEquals(jmsxgroupID, prop);
         }
         else
         {
            jmsxgroupID = prop;
         }
      }

      //All msgs should go to the first consumer
      for (int j = 0; j < 100; j++)
      {        
         TextMessage tm = (TextMessage)consumer1.receive(10000);
         
         assertNotNull(tm);
         
         assertEquals("Message" + j, tm.getText());
         
         assertEquals(tm.getStringProperty("JMSXGroupID"), jmsxgroupID);
      }
      
      connection.close();
      
      
   }

}
