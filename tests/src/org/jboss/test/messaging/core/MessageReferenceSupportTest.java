/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.MessageReferenceSupport;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageReferenceSupportTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public MessageReferenceSupportTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testEquals() throws Exception
   {
      // TODO not sure about that yet
      MessageReferenceSupport ref1 = new MessageReferenceSupport("ID", true, 0, "storeID");
      ref1.putHeader("someHeader", "someValue");
      MessageReferenceSupport ref2 = new MessageReferenceSupport("ID", false, 1, "storeID");
      assertEquals(ref1, ref2);
      assertEquals(ref1.hashCode(), ref2.hashCode());

   }
}
