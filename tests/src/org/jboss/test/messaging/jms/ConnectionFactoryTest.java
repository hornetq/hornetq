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

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionFactoryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public ConnectionFactoryTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }

   public void testSomething() throws Exception
   {
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
