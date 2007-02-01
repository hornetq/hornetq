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
import javax.jms.Destination;
import javax.management.MBeanServer;
import javax.naming.InitialContext;

import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * This is where we put individual tests that are no longer valid.
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class GraveyardTest extends MessagingTestCase
{
   private static final Logger log = Logger.getLogger(GraveyardTest.class);

   
   public GraveyardTest(String name)
   {
      super(name);
   }
   
   protected InitialContext initialContext;

   protected ConnectionFactory cf;
   
   protected Destination topic;


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

   
   /*
    * This test was excluded on 31/01/07 since we do not pass a session id in every invocation therefore
    * it's not possible to guarantee the "correct" one is used.
    * This is ok, since our remoting connector is exclusively ours, no one else will use it
    */
   
   /**
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
