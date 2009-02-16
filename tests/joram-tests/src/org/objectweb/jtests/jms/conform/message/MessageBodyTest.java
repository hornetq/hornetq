/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.conform.message;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

import org.objectweb.jtests.jms.framework.PTPTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Tests on message body.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: MessageBodyTest.java,v 1.1 2007/03/29 04:28:37 starksm Exp $
 */
public class MessageBodyTest extends PTPTestCase
{

   /**
    * Test that the <code>TextMessage.clearBody()</code> method does nto clear the 
    * message properties.
    */
   public void testClearBody_2()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setStringProperty("prop", "foo");
         message.clearBody();
         assertEquals("sec. 3.11.1 Clearing a message's body does not clear its property entries.\n",
                      "foo",
                      message.getStringProperty("prop"));
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that the <code>TextMessage.clearBody()</code> effectively clear the body of the message
    */
   public void testClearBody_1()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setText("bar");
         message.clearBody();
         assertEquals("sec. 3 .11.1 the clearBody method of Message resets the value of the message body " + "to the 'empty' initial message value as set by the message type's create "
                               + "method provided by Session.\n",
                      null,
                      message.getText());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /**
    * Test that a call to the <code>TextMessage.setText()</code> method on a 
    * received message raises a <code>javax.jms.MessageNotWriteableException</code>.
    */
   public void testWriteOnReceivedBody()
   {
      try
      {
         TextMessage message = senderSession.createTextMessage();
         message.setText("foo");
         sender.send(message);

         Message m = receiver.receive(TestConfig.TIMEOUT);
         assertTrue("The message should be an instance of TextMessage.\n", m instanceof TextMessage);
         TextMessage msg = (TextMessage)m;
         msg.setText("bar");
         fail("should raise a MessageNotWriteableException (sec. 3.11.2)");
      }
      catch (MessageNotWriteableException e)
      {
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   public MessageBodyTest(String name)
   {
      super(name);
   }
   
   @Override
   protected void setUp() throws Exception
   {
      
      super.setUp();
   }
   
   @Override
   protected void tearDown() throws Exception
   {      
      super.tearDown();
      
   }
}
