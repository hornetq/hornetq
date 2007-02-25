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
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class LongRunningInvocationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public LongRunningInvocationTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         throw new IllegalStateException("Test should only be run in remote mode");
      }
      
      super.setUp();
      
      ServerManagement.start("all");
      
      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.undeployQueue("JMSTestQueue");
      ServerManagement.deployQueue("JMSTestQueue");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      if (ServerManagement.isStarted(0))
      {
         ServerManagement.undeployQueue("JMSTestQueue");
      }
      
      ic.close();

      ServerManagement.kill(0);

      super.tearDown();
   }
   
   public void testLongRunningInvocationPingFirst() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/JMSTestQueue");

      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         MessageConsumer cons = session.createConsumer(queue);
   
         TextMessage m = session.createTextMessage("message one");
         
         // Pause a bit to let a least one ping through
         Thread.sleep(5000);
         
         //Poison the server so the invocation takes 2 minutes - longer than the ping period
         
         
       //  log.info("server is " + ServerManagement.getServer(0));
         ServerManagement.poisonTheServer(0, PoisonInterceptor.LONG_SEND);
         
         log.info("This will pause for 2 minutes on send");
         prod.send(m);
         
         conn.start();
         
         TextMessage m2 = (TextMessage)cons.receive(1000);
         
         assertEquals(m.getText(), m2.getText());
      }
      finally
      {
         conn.close();
      }

   }
   
   public void testLongRunningInvocation() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/JMSTestQueue");

      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         MessageConsumer cons = session.createConsumer(queue);
   
         TextMessage m = session.createTextMessage("message one");
         
         //Poison the server so the invocation takes 2 minutes - longer than the ping period
         
         //ServerManagement.poisonTheServer(0, PoisonInterceptor.LONG_SEND);
         
         log.info("This will pause for 2 minutes on send");
         prod.send(m);
         
         conn.start();
         
         TextMessage m2 = (TextMessage)cons.receive(1000);
         
         assertEquals(m.getText(), m2.getText());
      }
      finally
      {
         conn.close();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
