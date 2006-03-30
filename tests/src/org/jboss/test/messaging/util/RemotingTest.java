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
package org.jboss.test.messaging.util;

import javax.management.MBeanServer;

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

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * RemotingTest.java,v 1.1 2006/03/28 14:26:20 timfox Exp
 */
public class RemotingTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean connListenerCalled;

   // Constructors --------------------------------------------------

   public RemotingTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
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

   /**
    * TODO: Commented out until fixed. See http://jira.jboss.org/jira/browse/JBMESSAGING-287
    */
//   public void testConnectionListener() throws Throwable
//   {
//      // Start a server
//
//      Connector serverConnector = new Connector();
//
//      InvokerLocator serverLocator = new InvokerLocator("socket://localhost:9099");
//
//      serverConnector.setInvokerLocator(serverLocator.getLocatorURI());
//
//      serverConnector.create();
//
//      SimpleServerInvocationHandler invocationHandler = new SimpleServerInvocationHandler();
//
//      serverConnector.addInvocationHandler("JMS", invocationHandler);
//
//      serverConnector.setLeasePeriod(1000);
//
//      serverConnector.addConnectionListener(new SimpleConnectionListener());
//
//      serverConnector.start();
//
//      try
//      {
//         Client client = new Client(serverLocator);
//
//         client.connect();
//
//         Thread.sleep(5000);
//
//         client.disconnect();
//
//         // Connection Listener should now be called
//
//         Thread.sleep(5000);
//
//         assertTrue(connListenerCalled);
//      }
//      finally
//      {
//         serverConnector.stop();
//         serverConnector.destroy();
//      }
//
//   }

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

}



