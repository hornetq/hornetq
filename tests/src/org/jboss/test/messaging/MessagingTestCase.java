/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging;

import org.jboss.logging.Logger;
import junit.framework.TestCase;

/**
 * The base case for messaging tests.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class MessagingTestCase extends TestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   public MessagingTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      log.info("========= Start test: " + getName());
   }

   protected void tearDown() throws Exception
   {
      log.info("========== Stop test: " + getName());
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
