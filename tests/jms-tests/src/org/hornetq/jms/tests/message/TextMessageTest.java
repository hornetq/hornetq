/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests.message;


import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

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

   protected void assertEquivalent(Message m, int mode, boolean redelivery) throws JMSException
   {
      super.assertEquivalent(m, mode, redelivery);

      TextMessage tm = (TextMessage)m;
      assertEquals("this is the payload", tm.getText());
   }
}
