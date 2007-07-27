/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService;
import org.jboss.test.thirdparty.remoting.util.SimpleConnectionListener;

/**
 * Tests for http://jira.jboss.org/jira/browse/JBMESSAGING-787,
 * http://jira.jboss.org/jira/browse/JBREM-691. Test written entirely at the Remoting level. If
 * fails with Remoting 2.2.0.Alpha6.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientInvokerTimeoutTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientInvokerTimeoutTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;

   // Constructors ---------------------------------------------------------------------------------

   public ClientInvokerTimeoutTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * A remoting client created for "business use" will throw an unexpected SocketTimeoutException
    * after 1000 ms during a long invocation, even if it's not supposed to. If fails with Remoting
    * 2.2.0.Alpha6.
    */
   public void testUnexpectedSocketTimeoutException() throws Throwable
   {
      // This test doesn't make sense for HTTP, so shortcut it

      if (!"socket".equals(ServerManagement.getRemotingTransport(0)))
      {
         return;
      }

      Client client = null;

      try
      {
         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         client.connect();

         SimpleConnectionListener connListener = new SimpleConnectionListener();
         client.addConnectionListener(connListener, 3333);

         log.info("connection listener added, pinging will start in 3.3 secs");

         Thread.sleep(5000);

         log.info("first ping is done, send a long running invocation");

         // in 2.2.0.Alpha6 the client will send the invocation over a socket configured to time
         // out after 1000 ms, so this will throw java.net.SocketTimeoutException

         client.invoke(new Long(5000));

         Thread.sleep(7000);
      }
      finally
      {
         if (client != null)
         {
            client.disconnect();
         }
      }
   }

   /**
    * Same as testUnexpectedSocketTimeoutException(), slightly different setup.
    */
   public void testUnexpectedSocketTimeoutException2() throws Throwable
   {
      // This test doesn't make sense for HTTP, so shortcut it

      if (!"socket".equals(ServerManagement.getRemotingTransport(0)))
      {
         return;
      }

      Client client = null;

      try
      {
         // create a "business" client

         client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

         client.connect();

         final Client clientCopy = client;

         // send a "long running" invocation first on a separate thread, so we can setup the
         // connection validator at the same time

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  // this invocation will take 5 secs to complete
                  clientCopy.invoke(new Long(5000));
               }
               catch(Throwable t)
               {
                  log.error("invocation failed", t);
               }

            }
         }, "Lazy Invocation Thread").start();

         Thread.sleep(1000);

         SimpleConnectionListener connListener = new SimpleConnectionListener();
         client.addConnectionListener(connListener, 3333);

         log.info("connection listener added, pinging will start in 3.3 secs");

         Thread.sleep(4000);

         log.info("first ping is done, the answer to the first invocation should be back already");

         // in 2.2.0.Alpha6 the client will send the invocation over a socket configured to time
         // out after 1000 ms, so this will throw java.net.SocketTimeoutException

         client.invoke(new Long(5000));

         Thread.sleep(7000);

      }
      finally
      {
         if (client != null)
         {
            client.disconnect();
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      // Start the Remoting service in a configuration similar to the one in which the bug shows up

      String addr = ServiceContainer.getCurrentAddress();
      int port = PortUtil.findFreePort(addr);

      String serviceLocatorString =
         "socket://" + addr + ":" + port + "/?" +
            "clientSocketClass=org.jboss.jms.client.remoting.ClientSocketWrapper&" +
            "dataType=jms&" +
            "marshaller=org.jboss.jms.wireformat.JMSWireFormat&" +
            "serializationtype=jboss&" +
            "socket.check_connection=false&" +
            "unmarshaller=org.jboss.jms.wireformat.JMSWireFormat";

      ServiceAttributeOverrides sao = new ServiceAttributeOverrides();
      sao.put(ServiceContainer.REMOTING_OBJECT_NAME, "LocatorURI", serviceLocatorString);

      ServerManagement.start(0, "remoting", sao, true, false);

      // obtain the server locator from the service itself, so we won't have any doubts we use
      // the right one

      serverLocator = new InvokerLocator((String)ServerManagement.
         getAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "InvokerLocator"));

      // deploy a "lazy subsystem", that will delay invocations as long is it told to; used to
      // simulate long running invocations

      RemotingTestSubsystemService.
         deployService("org.jboss.test.thirdparty.remoting.LazySubsystem");

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

}
