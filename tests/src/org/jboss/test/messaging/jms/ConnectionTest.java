/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import java.util.Collection;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   protected Connection connection;

   // Constructors --------------------------------------------------

   public ConnectionTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf =
            (ConnectionFactory)initialContext.lookup("/messaging/ConnectionFactory");
      connection = cf.createConnection();
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      //connection.stop();
      connection = null;
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void testGetClientID() throws Exception
   {
      String clientID = connection.getClientID();

      Set clientIDs = ServerManagement.getConnections();
      assertEquals(1, clientIDs.size());
      assertEquals(clientID, clientIDs.iterator().next());
   }

   public void testSetClientID() throws Exception
   {
      try
      {
         connection.setClientID("something");
         fail("This should have failed");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
