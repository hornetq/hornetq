/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.jboss.remoting.CannotConnectException;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService;
import org.jboss.test.thirdparty.remoting.util.SimpleConnectionListener;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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
      catch(CannotConnectException e)
      {
         // This happens for HTTP clients - if the client throws CannotConnectException, we're fine,
         // this is what our FailoverValveInteceptor is looking after
      }
      catch(IOException e)
      {
         // This happens for socket clients - if the client throws IOException subclass, we're fine,
         // this is what our FailoverValveInteceptor is looking after
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
      catch(IOException e)
      {
         // This happens for bisocket clients - if the client throws IOException subclass, we're
         // fine, this is what our FailoverValveInteceptor is looking after
         
         // Note. The bisocket transport can make internal invocations and therefore have a pooled
         // connection available.
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

   public void testDeadClientWithLeasingButNoConnectionValidator() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      // add a dummy server-side ConnectionListener, to enable leasing on server

      ServerManagement.invoke(ServiceContainer.REMOTING_OBJECT_NAME, "addConnectionListener",
                              new Object[] { new SimpleConnectionListener() },
                              new String[] {"org.jboss.remoting.ConnectionListener"});


      // enable leasing on client
      Map conf = new HashMap();
      conf.put(Client.ENABLE_LEASE, Boolean.TRUE);
      conf.put(InvokerLocator.CLIENT_LEASE_PERIOD, "999");

      Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL, conf);

      client.connect();

      long leasePeriod = client.getLeasePeriod();

      assertEquals(999, leasePeriod);

      ServerManagement.kill(0);

      // wait long enough so we get into lease pinging trouble, send around 5 pings; the lease
      // pinger will still be pinging, thinking that the server is up. In my opinion, this is a
      // Remoting design flaw, but we can work around from Messaging, so it's ok. I am using this
      // test to detect a change in behavior of future remoting releases.

      int i = 0;
      long upperLimit = System.currentTimeMillis() + 5 * leasePeriod;
      while(System.currentTimeMillis() < upperLimit)
      {
         i++;
         assertEquals("attempt " + i, 999, client.getLeasePeriod());
         Thread.sleep(1000);
      }
   }

   public void testDeadClientWithLeasingAndConnectionValidator() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      // add a dummy server-side ConnectionListener, to enable leasing on server

      ServerManagement.invoke(ServiceContainer.REMOTING_OBJECT_NAME, "addConnectionListener",
                              new Object[] { new SimpleConnectionListener() },
                              new String[] {"org.jboss.remoting.ConnectionListener"});


      // enable leasing on client
      Map conf = new HashMap();
      conf.put(Client.ENABLE_LEASE, Boolean.TRUE);
      conf.put(InvokerLocator.CLIENT_LEASE_PERIOD, "999");

      Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL, conf);

      client.connect();

      SimpleConnectionListener connListener = new SimpleConnectionListener();
      client.addConnectionListener(connListener);

      ServerManagement.kill(0);

      // wait until failure is detected

      Throwable failure = connListener.getNextFailure(3000);
      assertNotNull(failure);

      // we simulate what Messaging is doing and we

      client.setDisconnectTimeout(0);
      client.disconnect();
      
      // the client should be "dead", in that both the connection validator and the lease pinger
      // are silenced

      assertEquals(-1, client.getPingPeriod());
      assertEquals(-1, client.getLeasePeriod());
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

      log.debug("setup done");

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

}
