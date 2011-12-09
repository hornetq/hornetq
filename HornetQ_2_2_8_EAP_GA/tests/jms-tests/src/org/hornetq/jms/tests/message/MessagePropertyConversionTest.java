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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.jms.tests.HornetQServerTestCase;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * 
 * Testing of message property conversion. See javax.jms.Message for details
 * 
 * @author <a href="mailto:afu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class MessagePropertyConversionTest extends HornetQServerTestCase
{
   // Attributes ----------------------------------------------------

   private Connection producerConnection, consumerConnection;

   private Session queueProducerSession, queueConsumerSession;

   private MessageProducer queueProducer;

   private MessageConsumer queueConsumer;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      producerConnection = getConnectionFactory().createConnection();
      consumerConnection = getConnectionFactory().createConnection();

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(HornetQServerTestCase.queue1);
      queueConsumer = queueConsumerSession.createConsumer(HornetQServerTestCase.queue1);

      consumerConnection.start();
   }

   @Override
   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();

      super.tearDown();
   }

   public void testBooleanConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      boolean myBool = true;
      m1.setBooleanProperty("myBool", myBool);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Boolean property can be read as boolean and String but not anything
      // else

      ProxyAssertSupport.assertEquals(myBool, m2.getBooleanProperty("myBool"));
      ProxyAssertSupport.assertEquals(String.valueOf(myBool), m2.getStringProperty("myBool"));

      try
      {
         m2.getByteProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShortProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getIntProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getLongProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDoubleProperty("myBool");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testByteConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      byte myByte = 13;
      m1.setByteProperty("myByte", myByte);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Byte property can be read as byte, short, int, long or String

      ProxyAssertSupport.assertEquals(myByte, m2.getByteProperty("myByte"));
      ProxyAssertSupport.assertEquals(myByte, m2.getShortProperty("myByte"));
      ProxyAssertSupport.assertEquals(myByte, m2.getIntProperty("myByte"));
      ProxyAssertSupport.assertEquals(myByte, m2.getLongProperty("myByte"));
      ProxyAssertSupport.assertEquals(String.valueOf(myByte), m2.getStringProperty("myByte"));

      try
      {
         m2.getBooleanProperty("myByte");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myByte");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDoubleProperty("myByte");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testShortConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      short myShort = 15321;
      m1.setShortProperty("myShort", myShort);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Short property can be read as short, int, long or String

      ProxyAssertSupport.assertEquals(myShort, m2.getShortProperty("myShort"));
      ProxyAssertSupport.assertEquals(myShort, m2.getIntProperty("myShort"));
      ProxyAssertSupport.assertEquals(myShort, m2.getLongProperty("myShort"));
      ProxyAssertSupport.assertEquals(String.valueOf(myShort), m2.getStringProperty("myShort"));

      try
      {
         m2.getByteProperty("myShort");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBooleanProperty("myShort");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myShort");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDoubleProperty("myShort");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testIntConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      int myInt = 0x71ab6c80;
      m1.setIntProperty("myInt", myInt);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Int property can be read as int, long or String

      ProxyAssertSupport.assertEquals(myInt, m2.getIntProperty("myInt"));
      ProxyAssertSupport.assertEquals(myInt, m2.getLongProperty("myInt"));
      ProxyAssertSupport.assertEquals(String.valueOf(myInt), m2.getStringProperty("myInt"));

      try
      {
         m2.getShortProperty("myInt");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByteProperty("myInt");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBooleanProperty("myInt");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myInt");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDoubleProperty("myInt");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testLongConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      long myLong = 0x20bf1e3fb6fa31dfL;
      m1.setLongProperty("myLong", myLong);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Long property can be read as long and String

      ProxyAssertSupport.assertEquals(myLong, m2.getLongProperty("myLong"));
      ProxyAssertSupport.assertEquals(String.valueOf(myLong), m2.getStringProperty("myLong"));

      try
      {
         m2.getIntProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShortProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByteProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBooleanProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDoubleProperty("myLong");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testFloatConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      float myFloat = Float.MAX_VALUE - 23465;
      m1.setFloatProperty("myFloat", myFloat);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Float property can be read as float, double or String

      ProxyAssertSupport.assertEquals(myFloat, m2.getFloatProperty("myFloat"), 0);
      ProxyAssertSupport.assertEquals(String.valueOf(myFloat), m2.getStringProperty("myFloat"));
      ProxyAssertSupport.assertEquals(myFloat, m2.getDoubleProperty("myFloat"), 0);

      try
      {
         m2.getIntProperty("myFloat");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShortProperty("myFloat");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getLongProperty("myFloat");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByteProperty("myFloat");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBooleanProperty("myFloat");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testDoubleConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      double myDouble = Double.MAX_VALUE - 72387633;
      m1.setDoubleProperty("myDouble", myDouble);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      // Double property can be read as double and String

      ProxyAssertSupport.assertEquals(myDouble, m2.getDoubleProperty("myDouble"), 0);
      ProxyAssertSupport.assertEquals(String.valueOf(myDouble), m2.getStringProperty("myDouble"));

      try
      {
         m2.getFloatProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getIntProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShortProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByteProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBooleanProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloatProperty("myDouble");
         ProxyAssertSupport.fail();
      }
      catch (MessageFormatException e)
      {
      }
   }

   public void testStringConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      boolean myBool = true;
      byte myByte = 13;
      short myShort = 15321;
      int myInt = 0x71ab6c80;
      long myLong = 0x20bf1e3fb6fa31dfL;
      float myFloat = Float.MAX_VALUE - 23465;
      double myDouble = Double.MAX_VALUE - 72387633;
      String myString = "abcdef&^*&!^ghijkl";

      m1.setStringProperty("myString", myString);

      queueProducer.send(HornetQServerTestCase.queue1, m1);
      Message m2 = queueConsumer.receive(2000);

      ProxyAssertSupport.assertEquals(myString, m2.getStringProperty("myString"));

      // Test String -> Numeric and bool conversions.

      // String property can be read as boolean, byte, short,
      // int, long, float, double and String, with the possibility to
      // throw a runtime exception if the primitive's valueOf method does not
      // accept the String as a valid representation of the primitive

      Message m3 = queueProducerSession.createMessage();

      m3.setStringProperty("myBool", String.valueOf(myBool));
      m3.setStringProperty("myByte", String.valueOf(myByte));
      m3.setStringProperty("myShort", String.valueOf(myShort));
      m3.setStringProperty("myInt", String.valueOf(myInt));
      m3.setStringProperty("myLong", String.valueOf(myLong));
      m3.setStringProperty("myFloat", String.valueOf(myFloat));
      m3.setStringProperty("myDouble", String.valueOf(myDouble));
      m3.setStringProperty("myIllegal", "xyz123");

      queueProducer.send(HornetQServerTestCase.queue1, m3);

      Message m4 = queueConsumer.receive(2000);

      ProxyAssertSupport.assertEquals(myBool, m4.getBooleanProperty("myBool"));
      ProxyAssertSupport.assertEquals(myByte, m4.getByteProperty("myByte"));
      ProxyAssertSupport.assertEquals(myShort, m4.getShortProperty("myShort"));
      ProxyAssertSupport.assertEquals(myInt, m4.getIntProperty("myInt"));
      ProxyAssertSupport.assertEquals(myLong, m4.getLongProperty("myLong"));
      ProxyAssertSupport.assertEquals(myFloat, m4.getFloatProperty("myFloat"), 0);
      ProxyAssertSupport.assertEquals(myDouble, m4.getDoubleProperty("myDouble"), 0);

      ProxyAssertSupport.assertEquals(false, m4.getBooleanProperty("myIllegal"));

      try
      {
         m4.getByteProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m4.getShortProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m4.getIntProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m4.getLongProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m4.getFloatProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m4.getDoubleProperty("myIllegal");
         ProxyAssertSupport.fail();
      }
      catch (NumberFormatException e)
      {
      }
   }

   public void testJMSXDeliveryCountConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();
      queueProducer.send(HornetQServerTestCase.queue1, m1);

      Message m2 = queueConsumer.receive(2000);

      int count = m2.getIntProperty("JMSXDeliveryCount");
      ProxyAssertSupport.assertEquals(String.valueOf(count), m2.getStringProperty("JMSXDeliveryCount"));
      ProxyAssertSupport.assertEquals(count, m2.getLongProperty("JMSXDeliveryCount"));
   }
}
