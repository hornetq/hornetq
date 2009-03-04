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
package org.jboss.test.messaging.jms;

import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

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
         qc = cf.createQueueConnection();
         QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         QueueReceiver qreceiver = qs.createReceiver(queue1, "targetMessage = TRUE");

         qc.start();

         TextMessage m = qs.createTextMessage();
         m.setText("one");
         m.setBooleanProperty("targetMessage", false);

         QueueSender qsender = qs.createSender(queue1);

         qsender.send(m);

         m.setText("two");
         m.setBooleanProperty("targetMessage", true);

         qsender.send(m);

         TextMessage rm = (TextMessage)qreceiver.receive(1000);

         assertEquals("two", rm.getText());
      }
      finally
      {
         if (qc != null)
         {
            qc.close();
         }
         Thread.sleep(2000);
         log.info("****** removing merssages");
         removeAllMessages(queue1.getQueueName(), true, 0);
         checkEmpty(queue1);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
