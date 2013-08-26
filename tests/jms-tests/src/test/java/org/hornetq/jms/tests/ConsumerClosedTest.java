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
package org.hornetq.jms.tests;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 */
public class ConsumerClosedTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testMessagesSentDuringClose() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         c.start();

         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(HornetQServerTestCase.queue1);

         for (int i = 0; i < ConsumerClosedTest.NUMBER_OF_MESSAGES; i++)
         {
            p.send(s.createTextMessage("message" + i));
         }

         log.debug("all messages sent");

         MessageConsumer cons = s.createConsumer(HornetQServerTestCase.queue1);
         cons.close();

         log.debug("consumer closed");

         // make sure that all messages are in queue

         assertRemainingMessages(ConsumerClosedTest.NUMBER_OF_MESSAGES);
      }
      finally
      {
         if (c != null)
         {
            c.close();
         }

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
