/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.message.RoutableSupport;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RoutableSupportTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public RoutableSupportTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testNullAsHeaderValue() throws Exception
   {
      RoutableSupport rs = new RoutableSupport("messageID");
      rs.putHeader("someHeader", null);
      assertTrue(rs.containsHeader("someHeader"));
      assertNull(rs.getHeader("someHeader"));
      assertNull(rs.removeHeader("someHeader"));
      assertFalse(rs.containsHeader("someHeader"));
   }
}
