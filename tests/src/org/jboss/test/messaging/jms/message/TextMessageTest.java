/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.test.messaging.jms.message;


import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A test that sends/receives text messages to the JMS provider and verifies their integrity.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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

   public void testClearProperties() throws Exception
   {
      ((TextMessage)message).setText("something");
      queueProd.send(message);

      TextMessage rm = (TextMessage)queueCons.receive();

      rm.clearProperties();

      assertEquals("something", rm.getText());
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
