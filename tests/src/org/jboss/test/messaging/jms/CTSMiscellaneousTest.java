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
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.message.SimpleJMSBytesMessage;
import org.jboss.test.messaging.jms.message.SimpleJMSMessage;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;

import javax.jms.Queue;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.QueueReceiver;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.naming.InitialContext;

/**
 * Safeguards for previously detected TCK failures.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CTSMiscellaneousTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;

   // Constructors --------------------------------------------------

   public CTSMiscellaneousTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testForeignByteMessage() throws Exception
   {
      ConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      Connection c =  cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = (Queue)ic.lookup("/queue/Queue");
      MessageProducer p = s.createProducer(queue);

      // create a Bytes foreign message
      SimpleJMSBytesMessage bfm = new SimpleJMSBytesMessage();

      p.send(bfm);

      MessageConsumer cons = s.createConsumer(queue);
      c.start();

      BytesMessage bm = (BytesMessage)cons.receive();
      assertNotNull(bm);

      c.close();
   }

   public void testJMSMessageIDChanged() throws Exception
   {
      ConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      Connection c =  cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = (Queue)ic.lookup("/queue/Queue");
      MessageProducer p = s.createProducer(queue);

      Message m = new SimpleJMSMessage();
      m.setJMSMessageID("something");

      p.send(m);

      assertFalse("something".equals(m.getJMSMessageID()));

      c.close();
   }

   /**
    * com.sun.ts.tests.jms.ee.all.queueconn.QueueConnTest line 171
    */
   public void test_1() throws Exception
   {
      QueueConnectionFactory qcf = (QueueConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/Queue");

      QueueConnection qc =  qcf.createQueueConnection();
      QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      QueueReceiver qreceiver = qs.createReceiver(queue, "targetMessage = TRUE");

      qc.start();

      TextMessage m = qs.createTextMessage();
      m.setText("one");
      m.setBooleanProperty("targetMessage", false);

      QueueSender qsender = qs.createSender(queue);

      qsender.send(m);

      m.setText("two");
      m.setBooleanProperty("targetMessage", true);

      qsender.send(m);

      TextMessage rm = (TextMessage)qreceiver.receive(1000);

      assertEquals("two", rm.getText());

      qc.close();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");

      ic.close();
      
      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
