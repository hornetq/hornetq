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
package org.jboss.test.messaging.jms.base;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ConnectionFactory connFactory;
   protected Connection conn;
   protected Session session;
   protected MessageProducer queueProd;
   protected MessageConsumer queueCons;

   protected Queue queue;
   protected Topic topic;


   // Constructors --------------------------------------------------

   public JMSTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      connFactory = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      conn = connFactory.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      queue = (Queue)ic.lookup("/queue/Queue");

      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      topic = (Topic)ic.lookup("/topic/Topic");

      queueProd = session.createProducer(queue);
      queueCons = session.createConsumer(queue);

      conn.start();

      ic.close();
   }

   public void tearDown() throws Exception
   {
      conn.close();
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");

      //ServerManagement.stop();

      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
