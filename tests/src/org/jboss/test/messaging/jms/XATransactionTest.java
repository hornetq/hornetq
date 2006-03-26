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

import javax.naming.InitialContext;
import javax.jms.Queue;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.transaction.UserTransaction;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class XATransactionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected Queue queue;

   // Constructors --------------------------------------------------
   
   public XATransactionTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void testSimpleTransactedSend() throws Exception
   {
      ConnectionFactory mcf = (ConnectionFactory)ic.lookup("java:/JCAConnectionFactory");
      Connection conn = mcf.createConnection();
      conn.start();

      UserTransaction ut = ServerManagement.getUserTransaction();

      ut.begin();

      Session s = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = s.createProducer(queue);
      Message m = s.createTextMessage("one");

      p.send(m);

      ut.commit();

      conn.close();

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      conn = cf.createConnection();
      s = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      conn.start();

      TextMessage rm = (TextMessage)s.createConsumer(queue).receiveNoWait();

      assertEquals("one", rm.getText());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("UserTransactionTestQueue");
      queue = (Queue)ic.lookup("/queue/UserTransactionTestQueue");
      drainDestination((ConnectionFactory)ic.lookup("/ConnectionFactory"), queue);

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("UserTransactionTestQueue");
      ic.close();
      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


