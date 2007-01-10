/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.aop;

import org.jboss.logging.Logger;

/**
 * Used to force a "poisoned" server to do all sorts of bad things. Used for testing.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class PoisonAspect
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PoisonAspect.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   /**
    * Used by the failover tests to kill server in the middle of an invocation.
    */
   public void haltTheServer()
   {
      log.info("Halting the server ...");

      Runtime.getRuntime().halt(1);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
