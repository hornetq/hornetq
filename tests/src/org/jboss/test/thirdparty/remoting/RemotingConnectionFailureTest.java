/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.Client;
import org.jboss.remoting.CannotConnectException;
import org.jboss.remoting.ConnectionListener;

import javax.management.ObjectName;
import java.io.IOException;

import EDU.oswego.cs.dl.util.concurrent.Channel;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingConnectionFailureTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;
   private ObjectName subsystemService;

   // Constructors ---------------------------------------------------------------------------------

   public RemotingConnectionFailureTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testDeadServerAfterPreviousInvocation() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

      client.connect();

      client.invoke("blah");

      assertEquals("blah", RemotingTestSubsystemService.
         getNextInvocationFromServer(subsystemService, 2000).getParameter());

      ServerManagement.kill(0);

      // wait a bit for the server to go down

      Thread.sleep(3000);

      // send another invocation

      try
      {
         client.invoke("bloh");
         fail("This should have failed!");
      }
      catch(IOException e)
      {
         // if the client throws IOException, we're fine, this is our FailoverValveInteceptor is
         // looking after
      }
   }

   public void testDeadServerNoPreviousInvocation() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

      client.connect();

      ServerManagement.kill(0);

      // wait a bit for the server to go down

      Thread.sleep(3000);

      // send the first invocation

      try
      {
         client.invoke("bleh");
         fail("This should have failed!");
      }
      catch(CannotConnectException e)
      {
         // if the client throws CannotConnectException, we're fine, this is our
         // FailoverValveInteceptor is looking after
      }
   }

   public void testInvocationAfterDeathDetectedByPinger() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

      client.connect();

      SimpleConnectionListener connListener = new SimpleConnectionListener();
      client.addConnectionListener(connListener);

      ServerManagement.kill(0);

      long pingPeriod = client.getPingPeriod();

      // wait for the failure to be detected

      Throwable failure = connListener.getNextFailure(2 * pingPeriod);
      assertNotNull(failure);

      // the client is in a "broken" state, an invocation on it should fail

      try
      {
         client.invoke("bluh");
         fail("this should've failed!");
      }
      catch(CannotConnectException e)
      {
         // this is what a broken client is supposed to throw.
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start(0, "remoting", null, true, false);

      String s = (String)ServerManagement.
         getAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "InvokerLocator");

      serverLocator = new InvokerLocator(s);

      // remote a server invocation handler

      subsystemService = RemotingTestSubsystemService.deployService();

   }

   protected void tearDown() throws Exception
   {
      serverLocator = null;

      if (ServerManagement.isStarted(0))
      {
         RemotingTestSubsystemService.undeployService(subsystemService);
         ServerManagement.stop(0);
      }

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class SimpleConnectionListener implements ConnectionListener
   {
      private Channel failures;

      public SimpleConnectionListener()
      {
         failures = new LinkedQueue();
      }

      public void handleConnectionException(Throwable throwable, Client client)
      {
         try
         {
            failures.put(throwable);
         }
         catch(InterruptedException e)
         {
            throw new RuntimeException("Failed to record failure", e);
         }
      }

      public Throwable getNextFailure(long timeout) throws InterruptedException
      {
         return (Throwable)failures.poll(timeout);
      }
   }
}
