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
public class JMSTimestampHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSTimestampHeaderTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testJMSTimestamp() throws Exception
   {
      Message m = queueProducerSession.createMessage();

      long t1 = System.currentTimeMillis();
      queueProducer.send(m);
      long t2 = System.currentTimeMillis();
      long timestamp = queueConsumer.receive().getJMSTimestamp();

      assertTrue(timestamp >= t1);
      assertTrue(timestamp <= t2);
   }

   public void testDisabledTimestamp() throws Exception
   {
      Message m = queueProducerSession.createMessage();

      queueProducer.setDisableMessageTimestamp(true);
      queueProducer.send(m);
      assertEquals(0l, queueConsumer.receive().getJMSTimestamp());
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
