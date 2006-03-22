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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * The most comprehensive, yet simple, unit test.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public JMSTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      ServerManagement.deployQueue("JMSTestQueue");


      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("JMSTestQueue");

      ic.close();

      super.tearDown();
   }

   public void test_NonPersistent_NonTransactional() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/JMSTestQueue");

      Connection conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      TextMessage m = session.createTextMessage("message one");

      prod.send(m);

      conn.close();

      conn = cf.createConnection();

      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      TextMessage rm = (TextMessage)cons.receive();

      assertEquals("message one", rm.getText());

      conn.close();
   }

   public void test_Persistent_NonTransactional() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/JMSTestQueue");

      Connection conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      TextMessage m = session.createTextMessage("message one");

      prod.send(m);

      conn.close();

      conn = cf.createConnection();

      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      TextMessage rm = (TextMessage)cons.receive();

      assertEquals("message one", rm.getText());

      conn.close();
   }

   public void test_NonPersistent_Transactional() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/JMSTestQueue");

      Connection conn = cf.createConnection();

      Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      TextMessage m = session.createTextMessage("message one");
      prod.send(m);
      m = session.createTextMessage("message two");
      prod.send(m);

      session.commit();

      conn.close();

      conn = cf.createConnection();

      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      TextMessage rm = (TextMessage)cons.receive();
      assertEquals("message one", rm.getText());
      rm = (TextMessage)cons.receive();
      assertEquals("message two", rm.getText());

      conn.close();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
