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

package org.hornetq.jms.tests;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.tests.util.RandomUtil;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public class NoLocalSubscriberTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test that a message created from the same connection than a nolocal consumer
    * can be sent by *another* connection and will be received by the nolocal consumer
    */
   public void testNoLocal() throws Exception
   {
      if (log.isTraceEnabled())
      {
         log.trace("testNoLocal");
      }

      Connection defaultConn = null;
      Connection newConn = null;

      try
      {
         defaultConn = JMSTestCase.cf.createConnection();
         Session defaultSess = defaultConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer defaultConsumer = defaultSess.createConsumer(HornetQServerTestCase.topic1);
         MessageConsumer noLocalConsumer = defaultSess.createConsumer(HornetQServerTestCase.topic1, null, true);
         MessageProducer defaultProd = defaultSess.createProducer(HornetQServerTestCase.topic1);
         
         defaultConn.start();

         String text = RandomUtil.randomString();
         // message is created only once from the same connection than the noLocalConsumer
         TextMessage messageSent = defaultSess.createTextMessage(text);
         for (int i = 0; i < 10; i++)
         {
            defaultProd.send(messageSent);            
         }

         Message received = null;
         for (int i = 0; i < 10; i++)
         {
            received = defaultConsumer.receive(5000);
            assertNotNull(received);
            assertEquals(text, ((TextMessage)received).getText());
         }

         newConn = JMSTestCase.cf.createConnection();
         Session newSession = newConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer newProd = newSession.createProducer(HornetQServerTestCase.topic1);
         MessageConsumer newConsumer = newSession.createConsumer(HornetQServerTestCase.topic1);

         newConn.start();
         
         text = RandomUtil.randomString();
         messageSent.setText(text);
         defaultProd.send(messageSent);

         received = newConsumer.receive(5000);
         assertNotNull(received);
         assertEquals(text, ((TextMessage)received).getText());

         text = RandomUtil.randomString();
         messageSent.setText(text);
         // we send the message created at the start of the test but on the *newConn* this time
         newProd.send(messageSent);
         newConn.close();
         
         received = noLocalConsumer.receive(5000);
         assertNotNull("nolocal consumer did not get message", received);
         assertEquals(text, ((TextMessage)received).getText());
      }
      finally
      {
         if (defaultConn != null)
         {
            defaultConn.close();
         }
         if (newConn != null)
         {
            newConn.close();
         }
      }
   }
}
