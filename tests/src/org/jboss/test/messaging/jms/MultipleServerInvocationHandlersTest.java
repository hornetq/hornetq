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
import javax.management.MBeanServer;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.jms.server.ServerPeer;

import java.util.Set;
import java.io.Serializable;

/**
 * Tests the situation when there is more than one ServerInvokerHandler registered with the
 * Remoting connector. Goes through a combination of most common use cases.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MultipleServerInvocationHandlersTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public MultipleServerInvocationHandlersTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testMessageRoundTrip() throws Exception
   {
      Set subsystems = ServerManagement.getConnectorSubsystems();
      assertEquals(2, subsystems.size());

      assertTrue(subsystems.contains("DEFAULT_INVOCATION_HANDLER"));
      assertTrue(subsystems.contains(ServerPeer.REMOTING_JMS_SUBSYSTEM));

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MultipleServerInvocationHandlerTestQueue");

      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      TextMessage m = session.createTextMessage("payload");

      prod.send(m);

      conn.close();

      conn = cf.createConnection();

      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      TextMessage rm = (TextMessage)cons.receive();

      assertEquals("payload", rm.getText());

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");
      
      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      // stop the Messaging server and re-start it after I register an extra remoting invocation
      // handler with the connector

      ServerManagement.stopServerPeer();

      Set subsystems = ServerManagement.getConnectorSubsystems();
      assertEquals(1, subsystems.size());
      assertTrue(subsystems.contains(ServerPeer.REMOTING_JMS_SUBSYSTEM));

      ServerManagement.addServerInvocationHandler("DEFAULT_INVOCATION_HANDLER",
                                                  new SimpleServerInvocationHandler());

      // restart the server peer so it will add its ServerInvocationHandler AFTER
      // SimpleServerInvocationHandler - this simulates the situation where the same Connector
      // has more than one ServerInvocationHandler instance
      ServerManagement.startServerPeer();

      ServerManagement.deployQueue("MultipleServerInvocationHandlerTestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      // remove the test invocation handler
      ServerManagement.removeServerInvocationHandler("DEFAULT_INVOCATION_HANDLER");
      ServerManagement.undeployQueue("MultipleServerInvocationHandlerTestQueue");
      ic.close();
      ServerManagement.stop();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class SimpleServerInvocationHandler
      implements ServerInvocationHandler, Serializable
   {
      private static final long serialVersionUID = 398759739753345L;

      public void setMBeanServer(MBeanServer server)
      {
      }

      public void setInvoker(ServerInvoker invoker)
      {
      }

      public Object invoke(InvocationRequest invocation) throws Throwable
      {
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
