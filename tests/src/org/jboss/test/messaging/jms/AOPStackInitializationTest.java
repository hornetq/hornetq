/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.JBMServerTestCase;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

/**
 * Tests the very first server invocation, when the client-side AOP stack is initialized.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1843 $</tt>
 *
 * $Id: JMSTest.java 1843 2006-12-21 23:41:19Z timfox $
 */
public class AOPStackInitializationTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public AOPStackInitializationTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSimpleInitialization() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Connection conn = cf.createConnection();

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ic = getInitialContext();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ic.close();


      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
