/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;

import junit.framework.TestCase;

/**
 * The base class for messaging tests.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class MessagingTestCase extends TestCase
{
   // Attributes ----------------------------------------------------

   /** The logger */
   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   public MessagingTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      log.info("========= Start test: " + getName());
      ServerManagement.setRemote(false);
   }

   protected void tearDown() throws Exception
   {
      log.info("========== Stop test: " + getName());
   }
}
