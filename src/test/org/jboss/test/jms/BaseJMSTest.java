
/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.jms;

import java.util.logging.Logger;

import junit.framework.TestCase;

/**
 * The base jms test
 * 
 * @author <a href="adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class BaseJMSTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The logger */
   protected Logger log = Logger.getLogger(getClass().getName());

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BaseJMSTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Protected ------------------------------------------------------

   protected void setUp()
      throws Exception
   {
      log.info("========= Start test: " + getName());
   }

   protected void tearDown()
      throws Exception
   {
      log.info("========== Stop test: " + getName());
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
