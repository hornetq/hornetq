
/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.jms;

import junit.framework.TestCase;

import org.jboss.logging.Logger;
import org.jboss.logging.XLevel;

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
   protected Logger log = Logger.getLogger(getClass());

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BaseJMSTest(String name)
   {
      super(name);
      org.apache.log4j.Logger jboss = org.apache.log4j.Logger.getLogger("org.jboss");
      jboss.setLevel(XLevel.ALL);
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
