/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import org.jboss.test.messaging.jms.message.base.MessageTestBase;

import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A test that sends/receives text messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TextMessageTest extends MessageTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TextMessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      message = session.createTextMessage();
   }

   public void tearDown() throws Exception
   {
      message = null;
      super.tearDown();
   }

   // Protected -----------------------------------------------------

   protected void prepareMessage(Message m) throws JMSException
   {
      super.prepareMessage(m);

      TextMessage tm = (TextMessage)m;
      tm.setText("this is the payload");
   }

   protected void assertEquivalent(Message m, int mode) throws JMSException
   {
      super.assertEquivalent(m, mode);

      TextMessage tm = (TextMessage)m;
      assertEquals("this is the payload", tm.getText());
   }
}
