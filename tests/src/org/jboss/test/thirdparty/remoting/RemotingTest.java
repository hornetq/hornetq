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
package org.jboss.test.thirdparty.remoting;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision: 1935 $</tt>
 *
 * $Id: RemotingTest.java 1935 2007-01-09 23:29:20Z clebert.suconic@jboss.com $
 */
public class RemotingTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   InitialContext ic;
   
   private boolean connListenerCalled;

   // Constructors ---------------------------------------------------------------------------------

   public RemotingTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testInvokerThreadSafety() throws Exception
   {
      Connector serverConnector = new Connector();

      InvokerLocator serverLocator = new InvokerLocator("socket://localhost:9099");

      serverConnector.setInvokerLocator(serverLocator.getLocatorURI());

      serverConnector.create();

      SimpleServerInvocationHandler invocationHandler = new SimpleServerInvocationHandler();

      serverConnector.addInvocationHandler("JMS", invocationHandler);

      serverConnector.start();

      // Create n clients each firing requests in their own thread, using the same locator

      try
      {

      final int NUM_CLIENTS = 3;

      Thread[] threads = new Thread[NUM_CLIENTS];
      Invoker[] invokers = new Invoker[NUM_CLIENTS];

      Object obj = new Object();

      for (int i = 0; i < NUM_CLIENTS; i++)
      {
         invokers[i] = new Invoker(serverLocator, obj);
         threads[i] = new Thread(invokers[i]);
         threads[i].start();
      }

      synchronized (obj)
      {
         obj.wait();
      }

      for (int i = 0; i < NUM_CLIENTS; i++)
      {
         if (invokers[i].failed)
         {
            fail();
            for (int j = 0; j < NUM_CLIENTS; j++)
            {
               threads[j].interrupt();
            }
         }
      }

      for (int i = 0; i < NUM_CLIENTS; i++)
      {
         threads[i].join();
      }

      for (int i = 0; i < NUM_CLIENTS; i++)
      {
         if (invokers[i].failed)
         {
            fail();
         }
      }
      }
      finally
      {
         serverConnector.stop();
         serverConnector.destroy();
      }
   }

   public void testConnectionListener() throws Throwable
   {
      // Start a server

      Connector serverConnector = new Connector();

      InvokerLocator serverLocator = new InvokerLocator("socket://localhost:9099");

      serverConnector.setInvokerLocator(serverLocator.getLocatorURI());

      serverConnector.create();

      SimpleServerInvocationHandler invocationHandler = new SimpleServerInvocationHandler();

      serverConnector.addInvocationHandler("JMS", invocationHandler);

      serverConnector.setLeasePeriod(1000);

      serverConnector.addConnectionListener(new SimpleConnectionListener());

      serverConnector.start();

      try
      {
         HashMap metadata = new HashMap();
         
         metadata.put(InvokerLocator.FORCE_REMOTE, "true");
         
         metadata.put(Client.ENABLE_LEASE, "true");
         
         Client client = new Client(serverLocator, metadata);

         client.connect();

         Thread.sleep(5000);

         client.disconnect();

         // Connection Listener should now be called

         Thread.sleep(5000);

         assertTrue(connListenerCalled);
      }
      finally
      {
         serverConnector.stop();
         serverConnector.destroy();
      }

   }

   /**
    * JIRA issue: http://jira.jboss.org/jira/browse/JBMESSAGING-371
    */
   public void testMessageListenerTimeout() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Destination topic = (Destination)ic.lookup("/topic/ATopic");

      Connection conn = cf.createConnection();
      Slot slot = new Slot();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(topic);
      consumer.setMessageListener(new SimpleMessageListener(slot));

      conn.start();

      Connection conn2 = cf.createConnection();
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session2.createProducer(topic);
      Message m = session.createTextMessage("blah");

      prod.send(m);

      TextMessage rm = (TextMessage)slot.poll(5000);

      assertEquals("blah", rm.getText());

      // Only for JBoss Remoting > 2.0.0.Beta1
      long sleepTime = ServerInvoker.DEFAULT_TIMEOUT_PERIOD + 60000;
      log.info("sleeping " + (sleepTime / 60000) + " minutes");

      Thread.sleep(sleepTime);

      log.info("after sleep");

      // send the second message. In case of remoting timeout, the callback server won't forward
      // this message to the MessageCallbackHandler, and the test will fail

      Message m2 = session.createTextMessage("blah2");
      prod.send(m2);

      TextMessage rm2 = (TextMessage)slot.poll(5000);

      assertNotNull(rm2);
      assertEquals("blah2", rm2.getText());

      conn.close();
      conn2.close();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployTopic("ATopic");

      log.debug("setup done");

   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("ATopic");

      ic.close();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


   class Invoker implements Runnable
   {
      boolean failed;
      InvokerLocator locator;
      Object o;
      Invoker(InvokerLocator locator, Object o)
      {
         this.locator = locator;
         this.o = o;
      }
      public void run()
      {
         try
         {
            for (int i = 0; i < 5000; i++)
            {
               Client cl = new Client(locator);
               cl.connect();
               cl.invoke("aardvark");
               cl.disconnect();
            }
            synchronized (o)
            {
               o.notify();
            }
         }
         catch (Throwable t)
         {
            failed = true;
            log.error("Caught throwable", t);
            synchronized (o)
            {
               o.notify();
            }
         }
      }
   }

   class SimpleConnectionListener implements ConnectionListener
   {
      public void handleConnectionException(Throwable t, Client client)
      {
         connListenerCalled = true;
      }
   }

   class SimpleServerInvocationHandler implements ServerInvocationHandler
   {
      InvokerCallbackHandler handler;


      public void addListener(InvokerCallbackHandler callbackHandler)
      {
         this.handler = callbackHandler;

      }

      public Object invoke(InvocationRequest invocation) throws Throwable
      {
         //log.info("Received invocation:" + invocation);

         return "Sausages";
      }

      public void removeListener(InvokerCallbackHandler callbackHandler)
      {
         // FIXME removeListener

      }

      public void setInvoker(ServerInvoker invoker)
      {
         // FIXME setInvoker

      }

      public void setMBeanServer(MBeanServer server)
      {
         // FIXME setMBeanServer

      }
   }


   private class SimpleMessageListener implements MessageListener
   {
      private Slot slot;
      private boolean failure;

      public SimpleMessageListener(Slot slot)
      {
         this.slot = slot;
         failure = false;
     }

      public void onMessage(Message m)
      {
         log.info("received " + m);
         try
         {
            slot.put(m);
         }
         catch(Exception e)
         {
            log.error("failed to put message in slot", e);
            failure = true;
         }
      }

      public boolean isFailure()
      {
         return failure;
      }
   }

}



