/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import EDU.oswego.cs.dl.util.concurrent.Channel;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.thirdparty.remoting.util.OnewayCallbackTrigger;
import org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService;

import javax.management.ObjectName;

/**
 * A test case in which we play with "pure" remoting asynchronous calls.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PureAsynchronousCallTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PureAsynchronousCallTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;

   // Constructors ---------------------------------------------------------------------------------

   public PureAsynchronousCallTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testAsynchronousDirectCall() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = null;
      ObjectName subsystemService = null;

      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         client.connect();

         client.invokeOneway("blip");

         // make sure invocation reached the target subsystem

         InvocationRequest i =
            RemotingTestSubsystemService.getNextInvocationFromServer(subsystemService, 2000);

         assertNotNull(i);
         assertEquals("blip", i.getParameter());
      }
      finally
      {
         if (client != null)
         {
            client.disconnect();
         }

         RemotingTestSubsystemService.undeployService(subsystemService);
      }
   }

   /**
    * Send a lot of oneway calls to make sure remoting returns sockets back to pool.
    */
   public void testManyOnewayCalls() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = null;
      ObjectName subsystemService = null;

      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         client.connect();

         // send a ton of invocations
         int COUNT = 200;

         for(int i = 0; i < COUNT; i++)
         {
            client.invokeOneway("ignore");
         }

         // wait a bit until all invocations are flushed from the server-side pool
         Thread.sleep(5000);
      }
      finally
      {
         if (client != null)
         {
            client.disconnect();
         }

         RemotingTestSubsystemService.undeployService(subsystemService);
      }
   }


   public void testAsynchronousCallback() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = null;
      ObjectName subsystemService = null;
      SimpleCallbackHandler callbackHandler = null;

      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         callbackHandler = new SimpleCallbackHandler();

         client.connect();

         client.addListener(callbackHandler, null, null, true);

         client.invoke(new OnewayCallbackTrigger("blop"));

         // make sure we get the callback

         Callback c = callbackHandler.getNextCallback(3000);

         assertNotNull(c);
         assertEquals("blop", c.getParameter());
      }
      finally
      {
         if (client != null)
         {
            client.disconnect();
         }

         RemotingTestSubsystemService.undeployService(subsystemService);
      }
   }


   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      // start "raw" remoting, don't use JBM configuration

      String addr = ServiceContainer.getCurrentAddress();
      int port = PortUtil.findFreePort(addr);
      String serviceLocatorString = "socket://" + addr + ":" + port ;
      ServiceAttributeOverrides sao = new ServiceAttributeOverrides();
      sao.put(ServiceContainer.REMOTING_OBJECT_NAME, "LocatorURI", serviceLocatorString);

      ServerManagement.start(0, "remoting", sao, true, false);

      String s = (String)ServerManagement.
         getAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "InvokerLocator");

      serverLocator = new InvokerLocator(s);

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      serverLocator = null;

      ServerManagement.stop();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class SimpleCallbackHandler implements InvokerCallbackHandler
   {
      private Channel callbackHistory;

      public SimpleCallbackHandler()
      {
         callbackHistory = new LinkedQueue();
      }

      public void handleCallback(Callback callback) throws HandleCallbackException
      {
         try
         {
            callbackHistory.put(callback);
         }
         catch(InterruptedException e)
         {
            throw new HandleCallbackException("Got InterruptedException", e);
         }
      }

      public Callback getNextCallback(long timeout) throws InterruptedException
      {
         return (Callback)callbackHistory.poll(timeout);
      }
   }

}
