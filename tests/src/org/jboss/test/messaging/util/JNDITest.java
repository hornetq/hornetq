/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import java.util.Hashtable;

import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDITest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JNDITest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public JNDITest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testInterServerJNDI() throws Exception
   {
      // this test doesn't make sense in a colocated topology.

      if (!isRemote())
      {
         return;
      }

      try
      {
         ServerManagement.start(0, "all", true);
         ServerManagement.start(1, "all", false);

         // deploy an initial context "consumer" service on server 0

         String serviceName = "test:service=JNDITesterService";
         ObjectName on = new ObjectName(serviceName);

         String serviceConfig =
         "<mbean code=\"org.jboss.test.messaging.util.JNDITesterService\"\n" +
            " name=\"" + serviceName + "\">\n" +
         "</mbean>";

         ServerManagement.deploy(serviceConfig);

         // Deploy something into the server 1 JNDI namespace

         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(1));

         ic.bind("/", "bingo");

         log.debug("deployed");

         // feed the service with an JNDI environment from server 1

         Hashtable environment = ServerManagement.getJNDIEnvironment(1);
         String s = (String)ServerManagement.
            invoke(on, "installAndUseJNDIEnvironment",
                   new Object[] { environment, "/" },
                   new String[] { "java.util.Hashtable", "java.lang.String" });

         assertEquals("bingo", s);

      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.kill(1);
         ServerManagement.stop(0);
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


}
