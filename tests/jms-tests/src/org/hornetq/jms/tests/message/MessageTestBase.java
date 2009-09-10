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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.jms.tests.HornetQServerTestCase;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MessageTestBase extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Message message;

   protected Connection conn;

   protected Session session;

   protected MessageProducer queueProd;

   protected MessageConsumer queueCons;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      conn = getConnectionFactory().createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProd = session.createProducer(queue1);
      queueCons = session.createConsumer(queue1);

      conn.start();
   }

   public void tearDown() throws Exception
   {
      conn.close();

      super.tearDown();
   }

   public void testNonPersistentSend() throws Exception
   {
      prepareMessage(message);

      queueProd.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      queueProd.send(message);

      log.debug("Message sent");

      Message r = queueCons.receive(2000);
      assertNotNull(r);

      log.debug("Message received");

      assertEquals(DeliveryMode.NON_PERSISTENT, r.getJMSDeliveryMode());

      assertEquivalent(r, DeliveryMode.NON_PERSISTENT);
   }

   public void testPersistentSend() throws Exception
   {
      prepareMessage(message);

      queueProd.setDeliveryMode(DeliveryMode.PERSISTENT);

      queueProd.send(message);

      Message r = queueCons.receive(1000);
      assertNotNull(r);

      assertEquals(DeliveryMode.PERSISTENT, r.getJMSDeliveryMode());

      assertEquivalent(r, DeliveryMode.PERSISTENT);
   }

   public void testRedelivery() throws Exception
   {
      prepareMessage(message);

      session.close();
      session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      queueProd = session.createProducer(queue1);
      queueProd.setDeliveryMode(DeliveryMode.PERSISTENT);
      queueCons = session.createConsumer(queue1);

      queueProd.send(message);

      Message r = queueCons.receive(500);

      assertEquivalent(r, DeliveryMode.PERSISTENT);

      queueCons.close();
      session.close();

      session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      queueCons = session.createConsumer(queue1);
      r = queueCons.receive(1000);

      assertEquivalent(r, DeliveryMode.PERSISTENT, true);

      r.acknowledge();

      assertNull(queueCons.receive(100));


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void prepareMessage(Message m) throws JMSException
   {
      m.setBooleanProperty("booleanProperty", true);
      m.setByteProperty("byteProperty", (byte)3);
      m.setDoubleProperty("doubleProperty", 4.0);
      m.setFloatProperty("floatProperty", 5.0f);
      m.setIntProperty("intProperty", 6);
      m.setLongProperty("longProperty", 7);
      m.setShortProperty("shortProperty", (short)8);
      m.setStringProperty("stringProperty", "this is a String property");      

      m.setJMSCorrelationID("this is the correlation ID");
      m.setJMSReplyTo(topic1);
      m.setJMSType("someArbitraryType");
   }


   private void assertEquivalent(Message m, int mode) throws JMSException
   {
      assertEquivalent(m, mode, false);
   }

   protected void assertEquivalent(Message m, int mode, boolean redelivered) throws JMSException
   {
      assertNotNull(m);
      assertEquals(true, m.getBooleanProperty("booleanProperty"));
      assertEquals((byte)3, m.getByteProperty("byteProperty"));
      assertEquals(new Double(4.0), new Double(m.getDoubleProperty("doubleProperty")));
      assertEquals(new Float(5.0f), new Float(m.getFloatProperty("floatProperty")));
      assertEquals(6, m.getIntProperty("intProperty"));
      assertEquals(7, m.getLongProperty("longProperty"));
      assertEquals((short)8, m.getShortProperty("shortProperty"));
      assertEquals("this is a String property", m.getStringProperty("stringProperty"));

      assertEquals("this is the correlation ID", m.getJMSCorrelationID());
      assertEquals(topic1, m.getJMSReplyTo());
      assertEquals("someArbitraryType", m.getJMSType());
      assertEquals(queue1, m.getJMSDestination());
      assertEquals("JMS Redelivered property", m.getJMSRedelivered(), redelivered);
      assertEquals(mode, m.getJMSDeliveryMode());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
