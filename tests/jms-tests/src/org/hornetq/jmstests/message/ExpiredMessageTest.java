/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.jmstests.message;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.logging.Logger;
import org.hornetq.jmstests.JMSTestCase;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ExpiredMessageTest extends JMSTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ExpiredMessageTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public void testSimpleExpiration() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = session.createProducer(queue1);
      prod.setTimeToLive(1);

      Message m = session.createTextMessage("This message will die");

      prod.send(m);

      // wait for the message to die

      Thread.sleep(250);

      MessageConsumer cons = session.createConsumer(queue1);

      conn.start();

      assertNull(cons.receive(2000));
      
      conn.close();
   }
   
   public void testExpiredAndLivingMessages() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(queue1);

      // sent 2 messages: 1 expiring, 1 living
      TextMessage livingMessage = session.createTextMessage("This message will live");
      TextMessage expiringMessage = session.createTextMessage("This message will expire");

      prod.setTimeToLive(1);
      prod.send(expiringMessage);

      prod.setTimeToLive(0);
      prod.send(livingMessage);
      
      // wait for the expiring message to die
      Thread.sleep(250);

      MessageConsumer cons = session.createConsumer(queue1);
      conn.start();

      // receive living message
      Message receivedMessage = cons.receive(1000);
      assertNotNull("did not receive living message", receivedMessage);
      assertTrue(receivedMessage instanceof TextMessage);
      assertEquals(livingMessage.getText(), ((TextMessage)receivedMessage).getText());

      // we do not receive the expiring message
      assertNull(cons.receive(1000));
      
      conn.close();
   }
   
   public void testManyExpiredMessagesAtOnce() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = session.createProducer(queue1);
      prod.setTimeToLive(1);
      
      Message m = session.createTextMessage("This message will die");
      
      final int MESSAGE_COUNT = 100;
   
      for (int i = 0; i < MESSAGE_COUNT; i++)
      {
         prod.send(m);
      }
      
      MessageConsumer cons = session.createConsumer(queue1);
      conn.start();
      
      assertNull(cons.receive(2000));
      
      conn.close();
   }

  

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
 
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
