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
package org.jboss.test.messaging.graveyard;

import java.io.Serializable;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;

import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TransactionManagerLocator;

/**
 * This is where we put individual tests that are no longer valid. They are kept here for historical
 * reasons, but they are not run as part of the testsuite, because most likely will fail.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class GraveyardTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(GraveyardTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InitialContext initialContext;
   private ConnectionFactory cf;
   private Destination topic;
   private Destination queue;

   // Constructors ---------------------------------------------------------------------------------

   public GraveyardTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * This test was excluded on 31/01/07 since we do not pass a remoting session ID in every
    * invocation therefore it's not possible to guarantee the "correct" one is used. This is ok,
    * since our remoting connector is exclusively ours, no one else will use it.
    *
    * Test create connection when there is another Remoting invocation handler registered with the
    * Connector. I uncovered this bug while trying to run TCK/integration tests. In real life
    * Messaging has to co-exist with other invocation handlers registered with the Unified invoker's
    * Connector.
    */
   public void testCreateConnectionMultipleRemotingInvocationHandlers() throws Exception
   {
      // stop the Messaging server and re-start it after I register an extra remoting invocation
      // handler with the connector

      ServerManagement.stopServerPeer();

      Set subsystems = ServerManagement.getConnectorSubsystems();
      assertTrue(subsystems.contains(ServerPeer.REMOTING_JMS_SUBSYSTEM));
      assertEquals(1, subsystems.size());

      ServerManagement.addServerInvocationHandler("DEFAULT_INVOCATION_HANDLER",
                                                  new SimpleServerInvocationHandler());
      subsystems = ServerManagement.getConnectorSubsystems();
      assertTrue(subsystems.contains(ServerPeer.REMOTING_JMS_SUBSYSTEM));
      assertTrue(subsystems.contains("DEFAULT_INVOCATION_HANDLER"));
      assertEquals(2, subsystems.size());

      try
      {
         // restart the server peer so it will add its ServerInvocationHandler AFTER
         // SimpleServerInvocationHandler - this simulates the situation where the same Connector
         // has more than one ServerInvocationHandler instance
         ServerManagement.startServerPeer();

         // We need to re-lookup the connection factory after server restart, the new connection
         // factory points to a different thing
         cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         Connection connection = cf.createConnection();
         connection.close();
      }
      finally
      {
         // remove the test invocation handler
         ServerManagement.removeServerInvocationHandler("DEFAULT_INVOCATION_HANDLER");
      }
   }

   /**
    * Test excluded on 02/01/2007.
    *
    * The Messaging-layer level changes required to fix
    * http://jira.jboss.org/jira/browse/JBMESSAGING-721 will modify again the behavior checked
    * here. The changes will be made public in 1.0.1.SP4 and 1.2.0.CR1. The way an XASession behaves
    * in absence of a JTA transaction is incompletely specified, so it is inevitable that different
    * implemenation would work differently. Ultimately, it's up to the JCA layer, not the JMS layer,
    * to enforce a certain behavior. For more discussions on the subject, please also read
    * http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577.
    *
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-410.
    */
   public void testSendNoGlobalTransaction() throws Exception
   {
      Transaction suspended = null;

      try
      {
         ServerManagement.deployQueue("MyQueue");

         // make sure there's no active JTA transaction

         suspended = TransactionManagerLocator.getInstance().locate().suspend();

         // send a message to the queue, using a JCA wrapper

         Queue queue = (Queue)initialContext.lookup("queue/MyQueue");

         ConnectionFactory mcf =
            (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");

         Connection conn = mcf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");

         p.send(m);

         log.debug("message sent");

         conn.close();

         // receive the message
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         conn = cf.createConnection();
         conn.start();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue);
         TextMessage rm = (TextMessage)c.receive(1000);

         assertEquals("one", rm.getText());

         conn.close();
      }
      finally
      {
         ServerManagement.undeployQueue("MyQueue");

         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }
      }
   }

   /**
    * Test excluded on 02/01/2007.
    *
    * The Messaging-layer level changes required to fix
    * http://jira.jboss.org/jira/browse/JBMESSAGING-721 will modify again the behavior checked
    * here. The changes will be made public in 1.0.1.SP4 and 1.2.0.CR1. The way an XASession behaves
    * in absence of a JTA transaction is incompletely specified, so it is inevitable that different
    * implemenation would work differently. Ultimately, it's up to the JCA layer, not the JMS layer,
    * to enforce a certain behavior. For more discussions on the subject, please also read
    * http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577.
    *
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-410. Use a cached connection that
    * was initally enroled in a global transaction.
    */
   public void testSendNoGlobalTransaction2() throws Exception
   {

      Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();

      try
      {

         ConnectionFactory mcf =
            (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");
         Connection conn = mcf.createConnection();
         conn.start();

         UserTransaction ut = ServerManagement.getUserTransaction();

         ut.begin();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         Message m = s.createTextMessage("one");

         p.send(m);

         ut.commit();

         conn.close();

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");
         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.start();

         TextMessage rm = (TextMessage)s.createConsumer(queue).receive(500);

         assertEquals("one", rm.getText());

         conn.close();

         // make sure there's no active JTA transaction

         assertNull(TransactionManagerLocator.getInstance().locate().getTransaction());

         // send a message to the queue, using a JCA wrapper

         mcf = (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");

         conn = mcf.createConnection();

         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         m = s.createTextMessage("one");

         p.send(m);

         conn.close();

         // receive the message
         cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         conn = cf.createConnection();
         conn.start();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue);
         rm = (TextMessage)c.receive(1000);

         assertEquals("one", rm.getText());

         conn.close();
      }
      finally
      {
         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }
      }
   }

   /**
    * Test excluded on 02/01/2007.
    *
    * The Messaging-layer level changes required to fix
    * http://jira.jboss.org/jira/browse/JBMESSAGING-721 will modify again the behavior checked
    * here. The changes will be made public in 1.0.1.SP4 and 1.2.0.CR1. The way an XASession behaves
    * in absence of a JTA transaction is incompletely specified, so it is inevitable that different
    * implemenation would work differently. Ultimately, it's up to the JCA layer, not the JMS layer,
    * to enforce a certain behavior. For more discussions on the subject, please also read
    * http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577.
    *
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-520.
    */
   public void testReceiveNoGlobalTransaction() throws Exception
   {
      try
      {
         ServerManagement.deployQueue("MyQueue2");

         // send a message to the queue

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         Queue queue = (Queue)initialContext.lookup("queue/MyQueue2");
         Connection conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         conn.close();

         // make sure there's no active JTA transaction

         Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();

         try
         {
            // using a JCA wrapper

            ConnectionFactory mcf =
               (ConnectionFactory)initialContext.lookup("java:/JCAConnectionFactory");
            conn = mcf.createConnection();
            conn.start();

            // no active JTA transaction here

            s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c = s.createConsumer(queue);

            // this method should send an untransacted acknowledgment that should clear the delivery
            TextMessage rm = (TextMessage)c.receive(1000);

            assertEquals("one", rm.getText());

            conn.close();

            // now the queue should be empty
            ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=MyQueue2");
            Integer count = (Integer)ServerManagement.getAttribute(on, "MessageCount");
            assertEquals(0, count.intValue());
         }
         finally
         {

            if (suspended != null)
            {
               TransactionManagerLocator.getInstance().locate().resume(suspended);
            }
         }
      }
      finally
      {
         ServerManagement.undeployQueue("MyQueue2");
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

      ServerManagement.undeployTopic("Topic");

      ServerManagement.deployTopic("Topic");

      topic = (Destination)initialContext.lookup("/topic/Topic");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("Topic");

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private static class SimpleServerInvocationHandler
      implements ServerInvocationHandler, Serializable
   {
      private static final long serialVersionUID = 23847329753297523L;

      public void setMBeanServer(MBeanServer server)
      {
      }

      public void setInvoker(ServerInvoker invoker)
      {
      }

      public Object invoke(InvocationRequest invocation) throws Throwable
      {
         log.error("received invocation " + invocation + ", " + invocation.getParameter());
         fail("This ServerInvocationHandler is not supposed to handle invocations");
         return null;
      }

      public void addListener(InvokerCallbackHandler callbackHandler)
      {
         fail("This ServerInvocationHandler is not supposed to add listeners");
      }

      public void removeListener(InvokerCallbackHandler callbackHandler)
      {
         fail("This ServerInvocationHandler is not supposed to remove listeners");
      }
   }
}
