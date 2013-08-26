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

package org.hornetq.jms.tests.message.foreign;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Message;
import javax.jms.TextMessage;

import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.jms.tests.message.MessageTestBase;
import org.hornetq.jms.tests.message.SimpleJMSMessage;
import org.hornetq.jms.tests.message.SimpleJMSTextMessage;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 *
 * Tests the delivery/receipt of a foreign message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 *
 */
public class ForeignMessageTest extends MessageTestBase
{
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      message = createForeignMessage();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      message = null;
   }

   protected Message createForeignMessage() throws Exception
   {
      SimpleJMSMessage m = new SimpleJMSMessage();
      log.debug("creating JMS Message type " + m.getClass().getName());

      return m;
   }

   @Test
   public void testForeignMessageSetDestination() throws Exception
   {
      // create a Bytes foreign message
      SimpleJMSTextMessage txt = new SimpleJMSTextMessage("hello from Brazil!");
      txt.setJMSDestination(null);

      queueProd.send(txt);

      ProxyAssertSupport.assertNotNull(txt.getJMSDestination());

      TextMessage tm = (TextMessage)queueCons.receive();
      ProxyAssertSupport.assertNotNull(tm);
      ProxyAssertSupport.assertEquals("hello from Brazil!", txt.getText());
   }



   @Test
   public void testForeignMessageCorrelationIDBytesDisabled() throws Exception
   {
      System.setProperty(HornetQJMSConstants.JMS_HORNETQ_ENABLE_BYTE_ARRAY_JMS_CORRELATION_ID_PROPERTY_NAME, "false");

      SimpleJMSMessage msg = new SimpleJMSMessage();

      msg.setJMSCorrelationID("mycorrelationid");
      byte[] bytes = new byte[] { 1, 4, 3, 6, 8};
      msg.setJMSCorrelationIDAsBytes(bytes);

      queueProd.send(msg);

      Message rec = queueCons.receive();
      ProxyAssertSupport.assertNotNull(rec);

      assertNull(rec.getJMSCorrelationIDAsBytes());

      assertEquals("mycorrelationid", msg.getJMSCorrelationID());
   }

   @Test
   public void testForeignMessageCorrelationID() throws Exception
   {
      System.setProperty(HornetQJMSConstants.JMS_HORNETQ_ENABLE_BYTE_ARRAY_JMS_CORRELATION_ID_PROPERTY_NAME, "true");

      SimpleJMSMessage msg = new SimpleJMSMessage();

      msg.setJMSCorrelationID("mycorrelationid");
      byte[] bytes = new byte[] { 1, 4, 3, 6, 8};
      msg.setJMSCorrelationIDAsBytes(bytes);

      queueProd.send(msg);

      Message rec = queueCons.receive();
      ProxyAssertSupport.assertNotNull(rec);

      //Bytes correlation id takes precedence
      byte[] bytesrec = rec.getJMSCorrelationIDAsBytes();

      assertByteArraysEqual(bytes, bytesrec);

      assertNull(rec.getJMSCorrelationID());
   }

   private void assertByteArraysEqual(final byte[] bytes1, final byte[] bytes2)
   {
      if (bytes1 == null | bytes2 == null)
      {
         ProxyAssertSupport.fail();
      }

      if (bytes1.length != bytes2.length)
      {
         ProxyAssertSupport.fail();
      }

      for (int i = 0; i < bytes1.length; i++)
      {
         ProxyAssertSupport.assertEquals(bytes1[i], bytes2[i]);
      }

   }

}
