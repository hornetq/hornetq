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

   public void testFailureToSpawnStartedServer() throws Exception
   {
      try
      {
         ServerManagement.start("all", 0);

         try
         {
            ServerManagement.spawn(0);
            fail("It should have failed!");
         }
         catch(Exception e)
         {
            // OK
         }
      }
      finally
      {
         ServerManagement.stop(0);
      }
   }

   public void testFailureToSpawnExistingRemoteServer() throws Exception
   {
      if (ServerManagement.isLocal())
      {
         // irrelevant for a colocated configuration
         return;
      }

      // this assumes that the remote server 0 has been started externally by and or by a script

      try
      {
         ServerManagement.spawn(0);
         fail("This should have failed!");
      }
      catch(Exception e)
      {
         // OK
      }
   }

   public void testSimpleSpawn() throws Exception
   {
      try
      {
         ServerManagement.spawn(7);

         ServerManagement.start("all", 7);

         Integer index = (Integer)ServerManagement.
            getAttribute(7, new ObjectName("jboss.messaging:service=ServerPeer"), "serverPeerID");

         assertEquals(7, index.intValue());

         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(7));

         ic.bind("/xxx", "yyy");

         assertEquals("yyy", ic.lookup("/xxx"));
      }
      finally
      {
         ServerManagement.kill(7);
      }
   }

   /**
    * Needs to be run in clustered mode.
    */
   public void testRessurect() throws Exception
   {

      if (!ServerManagement.isClustered())
      {
         fail("This test must be run in clustered mode!");
      }

      ServerManagement.start("all", 1);

      ServerManagement.kill(1);

      // wait a bit for the server to die
      
      log.info("Sleeping for 10 seconds ...");

      Thread.sleep(10000);

      // resurrect the server

      ServerManagement.spawn(1);
      ServerManagement.start("all", 1);

      Integer index = (Integer)ServerManagement.
         getAttribute(1, new ObjectName("jboss.messaging:service=ServerPeer"), "serverPeerID");

      assertEquals(1, index.intValue());
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
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
