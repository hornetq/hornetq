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

import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class QueueReceiverTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * com.sun.ts.tests.jms.ee.all.queueconn.QueueConnTest line 171
    */
   public void testCreateReceiverWithMessageSelector() throws Exception
   {
      QueueConnection qc = null;

      try
      {
         qc = JMSTestCase.cf.createQueueConnection();
         QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         QueueReceiver qreceiver = qs.createReceiver(HornetQServerTestCase.queue1, "targetMessage = TRUE");

         qc.start();

         TextMessage m = qs.createTextMessage();
         m.setText("one");
         m.setBooleanProperty("targetMessage", false);

         QueueSender qsender = qs.createSender(HornetQServerTestCase.queue1);

         qsender.send(m);

         m.setText("two");
         m.setBooleanProperty("targetMessage", true);

         qsender.send(m);

         TextMessage rm = (TextMessage)qreceiver.receive(1000);

         ProxyAssertSupport.assertEquals("two", rm.getText());
      }
      finally
      {
         if (qc != null)
         {
            qc.close();
         }
         Thread.sleep(2000);
         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
         checkEmpty(HornetQServerTestCase.queue1);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
