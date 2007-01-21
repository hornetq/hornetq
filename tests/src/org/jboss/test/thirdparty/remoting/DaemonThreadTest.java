/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import org.jboss.test.messaging.MessagingTestCase;

/**
 * This test makes sure that all client-side threads employed by Messaging are well behaved and
 * don't prevent a client from exiting cleanly.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DaemonThreadTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DaemonThreadTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testNothingYet() throws Throwable
   {
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
