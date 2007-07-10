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

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Map;
import java.util.HashSet;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.callback.CallbackPoller;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 *  
 * @author <a href="ron.sigal@jboss.com">Ron Sigal</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class RemotingConnectionConfigurationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionFactory cf;

   // Constructors --------------------------------------------------

   public RemotingConnectionConfigurationTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   // Public --------------------------------------------------------

   /**
    * It only makes sense to run remote. Exclude it from "invm-tests" target configuration.
    */
   public void testDefaultHTTPCallbackPollPeriod() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }

      JBossConnection connection = null;

      try
      {
         connection = (JBossConnection)cf.createConnection();
         connection.start();

         ClientConnectionDelegate delegate = (ClientConnectionDelegate)connection.getDelegate();
         JMSRemotingConnection remotingConnection = delegate.getRemotingConnection();
         Client client = remotingConnection.getRemotingClient();

         Field field = JMSRemotingConnection.class.getDeclaredField("serverLocator");
         field.setAccessible(true);
         InvokerLocator locator = (InvokerLocator)field.get(remotingConnection);
         String transport = locator.getProtocol();

         if (!"http".equals(transport))
         {
            // not interesting
            return;
         }

         field = Client.class.getDeclaredField("callbackPollers");
         field.setAccessible(true);
         Map callbackPollers = (Map)field.get(client);
         assertEquals(1, callbackPollers.size());

         CallbackPoller callbackPoller = (CallbackPoller)callbackPollers.values().iterator().next();

         field = CallbackPoller.class.getDeclaredField("pollPeriod");
         field.setAccessible(true);
         Long pollPeriod = (Long)field.get(callbackPoller);
         assertEquals(ServiceContainer.HTTP_CONNECTOR_CALLBACK_POLL_PERIOD, pollPeriod.longValue());
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }


   /**
    * It only makes sense to run remote. Exclude it from "invm-tests" target configuration.
    */
   public void testConnectionConfiguration() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }

      JBossConnection connection = null;

      try
      {
         String address = InetAddress.getLocalHost().getHostAddress();
         System.setProperty("jboss.messaging.callback.bind.address", address);
         
         int freePort = PortUtil.findFreePort(InetAddress.getLocalHost().getHostName());
         System.setProperty("jboss.messaging.callback.bind.port", Integer.toString(freePort));

         String pollPeriod = "654";
         System.setProperty("jboss.messaging.callback.pollPeriod", pollPeriod);

         System.setProperty("jboss.messaging.callback.reportPollingStatistics", "true");
         
         connection = (JBossConnection)cf.createConnection();
         connection.start();

         ClientConnectionDelegate delegate = (ClientConnectionDelegate)connection.getDelegate();
         JMSRemotingConnection remotingConnection = delegate.getRemotingConnection();
         Client client = remotingConnection.getRemotingClient();

         Field field = JMSRemotingConnection.class.getDeclaredField("serverLocator");
         field.setAccessible(true);
         InvokerLocator locator = (InvokerLocator)field.get(remotingConnection);
         String transport = locator.getProtocol();

         if ("socket".equals(transport)
               || "sslsocket".equals(transport)
               || "bisocket".equals(transport)
               || "sslbisocket".equals(transport))
         {
            field = Client.class.getDeclaredField("callbackConnectors");
            field.setAccessible(true);
            Map callbackConnectors = (Map)field.get(client);

            InvokerCallbackHandler callbackHandler = remotingConnection.getCallbackManager();
            HashSet map = (HashSet) callbackConnectors.get(callbackHandler);
            Connector connector = (Connector)map.iterator().next();
            locator = new InvokerLocator(connector.getInvokerLocator());
            assertEquals(address, locator.getHost());
            assertEquals(freePort, locator.getPort());
         }
         else if ("http".equals(transport))
         {
            field = Client.class.getDeclaredField("callbackPollers");
            field.setAccessible(true);
            Map callbackPollers = (Map)field.get(client);
            assertEquals(1, callbackPollers.size());

            CallbackPoller callbackPoller =
               (CallbackPoller)callbackPollers.values().iterator().next();

            field = CallbackPoller.class.getDeclaredField("pollPeriod");
            field.setAccessible(true);

            assertEquals(pollPeriod, ((Long)field.get(callbackPoller)).toString());
            
            field = CallbackPoller.class.getDeclaredField("reportStatistics");
            field.setAccessible(true);
            assertEquals(true, ((Boolean) field.get(callbackPoller)).booleanValue());
         }
         else
         {
            fail("Unrecognized transport: " + transport);
         }
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}
