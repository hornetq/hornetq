/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import javax.jms.Message;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSMessageIDHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSMessageIDHeaderTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testJMSMessageIDPrefix() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);
      String messageID = queueConsumer.receive().getJMSMessageID();
      // JMS1.1 specs 3.4.3
      assertTrue(messageID.startsWith("ID:"));
   }
   
   public void testJMSMessageIDChangedAfterSendingMessage() throws Exception
   {
      try
      {
         Message m = queueProducerSession.createMessage();;
         m.setJMSMessageID("ID:something");

         queueProducer.send(m);

         assertFalse("ID:something".equals(m.getJMSMessageID()));

      }
      finally
      {
         removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
