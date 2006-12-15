/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.naming.InitialContext;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerManagementTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ServerManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSpawnServer() throws Exception
   {
      try
      {
         ServerManagement.start(0, "all");

         ServerManagement.start(0, "all");
      }
      finally
      {
         ServerManagement.kill(0);
      }
   }

   public void testSimpleSpawn() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         fail("This test must be run in remote mode!");
      }

      try
      {
         log.info("Waiting for server 7 to start ...");

         ServerManagement.start(7, "all");

         log.info("Server 7 started");

         Integer index = (Integer)ServerManagement.
            getAttribute(7, new ObjectName("jboss.messaging:service=ServerPeer"), "serverPeerID");

         assertEquals(7, index.intValue());

         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(7));

         ic.bind("/xxx", "yyy");

         assertEquals("yyy", ic.lookup("/xxx"));
      }
      finally
      {
         log.info("Killing server 7");
         ServerManagement.kill(7);
      }
   }

   public void testRessurect() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         fail("This test must be run in remote mode!");
      }

      try
      {
         ServerManagement.start(1, "all");

         ServerManagement.kill(1);

         // wait a bit for the server to die

         log.info("Sleeping for 10 seconds ...");

         Thread.sleep(10000);

         // resurrect the server

         ServerManagement.start(1, "all");

         Integer index = (Integer)ServerManagement.
            getAttribute(1, new ObjectName("jboss.messaging:service=ServerPeer"), "serverPeerID");

         assertEquals(1, index.intValue());

         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(1));

         ic.bind("/xxx", "yyy");

         assertEquals("yyy", ic.lookup("/xxx"));
      }
      finally
      {
         ServerManagement.kill(1);
      }
   }

   public void testA()
   {
      System.out.println("A");
   }

   public void testB()
   {
      System.out.println("B");
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();

      // TODO: clean up spawned servers

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
