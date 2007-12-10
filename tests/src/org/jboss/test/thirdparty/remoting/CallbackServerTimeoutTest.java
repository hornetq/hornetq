/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import EDU.oswego.cs.dl.util.concurrent.Channel;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.wireformat.JMSWireFormat;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.thirdparty.remoting.util.OnewayCallbackTrigger;
import org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService;

import javax.management.ObjectName;

/**
 * An extra test for the same root problem that causes
 * http://jira.jboss.org/jira/browse/JBMESSAGING-371. The callback server seems to timeout never
 * to be heard from it again.
 *
 * @author <a href="mailto:ovidiu@svjboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CallbackServerTimeoutTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(CallbackServerTimeoutTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;
   private boolean firstTime = true;

   // Constructors ---------------------------------------------------------------------------------

   public CallbackServerTimeoutTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testTimeoutOnewayCallback() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = null;
      ObjectName subsystemService = null;
      CallbackServerTimeoutTest.SimpleCallbackHandler callbackHandler = null;

      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         callbackHandler = new SimpleCallbackHandler();

         client.connect();

         JMSRemotingConnection.
            addInvokerCallbackHandler("test", client, null, serverLocator, callbackHandler);

         client.invoke(new OnewayCallbackTrigger("blip"));

         // make sure we get the callback

         Callback c = callbackHandler.getNextCallback(3000);

         assertNotNull(c);
         assertEquals("blip", c.getParameter());

         // sleep for twice the timeout, to be sure
         long sleepTime = ServerInvoker.DEFAULT_TIMEOUT_PERIOD + 60000;
         log.info("sleeping for " + (sleepTime / 60000) + " minutes ...");

         Thread.sleep(sleepTime);

         log.debug("woke up");

         client.invoke(new OnewayCallbackTrigger("blop"));

         // make sure we get the callback

         c = callbackHandler.getNextCallback(3000);

         assertNotNull(c);
         assertEquals("blop", c.getParameter());

      }
      finally
      {
         if (client != null)
         {
            // Note. Calling Client.disconnect() does remove the InvokerCallbackHandler registered
            // above. For the http transport, the CallbackPoller will continue running, which will
            // generate a lot of ERROR log messages after the server has shut down.
            client.removeListener(callbackHandler);
            client.disconnect();
         }

         RemotingTestSubsystemService.undeployService(subsystemService);
      }
   }

   public void testTimeoutOnewayCallback2() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = null;
      ObjectName subsystemService = null;
      CallbackServerTimeoutTest.SimpleCallbackHandler callbackHandler = null;

      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         callbackHandler = new SimpleCallbackHandler();

         client.connect();

         JMSRemotingConnection.
            addInvokerCallbackHandler("test", client, null, serverLocator, callbackHandler);

         log.info("added listener");

         // sleep for twice the timeout, to be sure
         long sleepTime = ServerInvoker.DEFAULT_TIMEOUT_PERIOD + 60000;

         client.invoke(new OnewayCallbackTrigger("blip", new long[] { 0, sleepTime + 10000 }));

         log.info("sent invocation");

         // make sure we get the callback

         Callback c = callbackHandler.getNextCallback(3000);

         assertNotNull(c);
         assertEquals("blip", c.getParameter());

         log.info("sleeping for " + (sleepTime / 60000) + " minutes ...");

         Thread.sleep(sleepTime);

         log.debug("woke up");

         // make sure we get the second callback

         c = callbackHandler.getNextCallback(20000);

         assertNotNull(c);
         assertEquals("blip1", c.getParameter());

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

      // This test needs a special server, so make sure no other test left a server running.
      if (firstTime)
      {
         firstTime = false;
         ServerManagement.start(0, "remoting", null, true, false);
         ServerManagement.stop();
      }

      // start a "standard" (messaging-enabled) remoting, we need to strip off the
      // marshaller/unmarshaller, though, since it can only bring trouble to this test ...
      ServiceAttributeOverrides sao = new ServiceAttributeOverrides();
      sao.put(ServiceContainer.REMOTING_OBJECT_NAME,
              ServiceContainer.DO_NOT_USE_MESSAGING_MARSHALLERS, Boolean.TRUE);
      
      
      JMSWireFormat wf = new JMSWireFormat();
      
      MarshalFactory.addMarshaller("jms", wf, wf);      

      ServerManagement.start(0, "remoting", sao, true, false);

      String s = (String)ServerManagement.
         getAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "InvokerLocator");

      serverLocator = new InvokerLocator(s);
      log.info("InvokerLocator: " + serverLocator);

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
