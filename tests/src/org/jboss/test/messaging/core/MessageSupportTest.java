/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.message.MessageSupport;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageSupportTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public MessageSupportTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testEquals() throws Exception
   {
      // TODO not sure about that yet
      MessageSupport m1 = new MessageSupport("ID", true, 0);
      m1.putHeader("someHeader", "someValue");
      MessageSupport m2 = new MessageSupport("ID", false, 1);
      assertEquals(m1, m2);
      assertEquals(m1.hashCode(), m2.hashCode());

   }
}
