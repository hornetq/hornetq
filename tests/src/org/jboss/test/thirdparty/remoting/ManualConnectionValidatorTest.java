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
import org.jboss.test.thirdparty.remoting.util.SimpleConnectionListener;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.Client;

/**
 * This test sets the connection validator ping period to a very small value, and then let the
 * client ping for a long time. To be used with netstat to see if it creates new sockets for each
 * invocation.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ManualConnectionValidatorTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;

   // Constructors ---------------------------------------------------------------------------------

   public ManualConnectionValidatorTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testLotsOfPings() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }

      Client client = new Client(serverLocator);

      client.connect();

      SimpleConnectionListener connListener = new SimpleConnectionListener();
      client.addConnectionListener(connListener, 50);

      assertEquals(50, client.getPingPeriod());

      // let it ping for a while

      log.info("pinging for 60 seconds with a rate of a ping per 50 ms ...");
      Thread.sleep(60000);
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

   }

   protected void tearDown() throws Exception
   {
      serverLocator = null;

      if (ServerManagement.isStarted(0))
      {
         ServerManagement.stop(0);
      }

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
