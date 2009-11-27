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

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashSet;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.hornetq.jms.tests.HornetQServerTestCase;

/**
 * 
 * A MessageBodyTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 * 
 */
public class MessageBodyTest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Connection producerConnection, consumerConnection;

   protected Session queueProducerSession, queueConsumerSession;

   protected MessageProducer queueProducer;

   protected MessageConsumer queueConsumer;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      producerConnection = getConnectionFactory().createConnection();
      consumerConnection = getConnectionFactory().createConnection();

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue1);
      queueConsumer = queueConsumerSession.createConsumer(queue1);

      consumerConnection.start();
   }

   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();

      super.tearDown();
   }

   public void testSMBodyReadable() throws Exception
   {
      byte bValue = 123;
      StreamMessage sm = queueProducerSession.createStreamMessage();
      sm.writeByte(bValue);
      sm.setStringProperty("COM_SUN_JMS_TESTNAME", "xMessageEOFExceptionQTestforStreamMessage");
      queueProducer.send(sm);

      StreamMessage received = (StreamMessage)queueConsumer.receive(3000);
      received.readByte();
   }

   public void testBytesMessage() throws Exception
   {
      BytesMessage m = queueProducerSession.createBytesMessage();

      // some arbitrary values
      boolean myBool = true;
      byte myByte = -111;
      short myShort = 15321;
      int myInt = 0x71ab6c80;
      long myLong = 0x20bf1e3fb6fa31dfL;
      float myFloat = Float.MAX_VALUE - 23465;
      double myDouble = Double.MAX_VALUE - 72387633;
      String myString = "abcdef&^*&!^ghijkl\uD5E2\uCAC7\uD2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";
      log.trace("String is length:" + myString.length());
      char myChar = 'q';
      byte[] myBytes = new byte[] { -23, 114, -126, -12, 74, 87 };

      m.writeBoolean(myBool);
      m.writeByte(myByte);
      m.writeShort(myShort);
      m.writeChar(myChar);
      m.writeInt(myInt);
      m.writeLong(myLong);
      m.writeFloat(myFloat);
      m.writeDouble(myDouble);
      m.writeUTF(myString);
      m.writeBytes(myBytes);
      m.writeBytes(myBytes, 2, 3);

      m.writeObject(new Boolean(myBool));
      m.writeObject(new Byte(myByte));
      m.writeObject(new Short(myShort));
      m.writeObject(new Integer(myInt));
      m.writeObject(new Long(myLong));
      m.writeObject(new Float(myFloat));
      m.writeObject(new Double(myDouble));
      m.writeObject(myString);
      m.writeObject(myBytes);

      try
      {
         m.writeObject(new Object());
         fail();
      }
      catch (MessageFormatException e)
      {
         // OK
      }

      // Reading should not be possible when message is read-write
      try
      {
         m.readBoolean();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readShort();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readChar();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readInt();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readLong();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readFloat();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readDouble();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readUTF();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readUnsignedByte();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.readUnsignedShort();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         byte[] bytes = new byte[333];
         m.readBytes(bytes);
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         byte[] bytes = new byte[333];
         m.readBytes(bytes, 111);
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }
      try
      {
         m.getBodyLength();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
         // OK
      }

      queueProducer.send(queue1, m);

      BytesMessage m2 = (BytesMessage)queueConsumer.receive(2000);

      assertNotNull(m2);

      assertEquals(myBool, m2.readBoolean());
      assertEquals(myByte, m2.readByte());
      assertEquals(myShort, m2.readShort());
      assertEquals(myChar, m2.readChar());
      assertEquals(myInt, m2.readInt());
      assertEquals(myLong, m2.readLong());
      assertEquals(myFloat, m2.readFloat(), 0);
      assertEquals(myDouble, m2.readDouble(), 0);
      assertEquals(myString, m2.readUTF());

      byte[] bytes = new byte[6];
      int ret = m2.readBytes(bytes);
      assertEquals(6, ret);

      assertByteArraysEqual(myBytes, bytes);

      byte[] bytes2 = new byte[3];
      ret = m2.readBytes(bytes2);

      assertEquals(3, ret);

      assertEquals(myBytes[2], bytes2[0]);
      assertEquals(myBytes[3], bytes2[1]);
      assertEquals(myBytes[4], bytes2[2]);

      assertEquals(myBool, m2.readBoolean());
      assertEquals(myByte, m2.readByte());
      assertEquals(myShort, m2.readShort());
      assertEquals(myInt, m2.readInt());
      assertEquals(myLong, m2.readLong());
      assertEquals(myFloat, m2.readFloat(), 0);
      assertEquals(myDouble, m2.readDouble(), 0);
      assertEquals(myString, m2.readUTF());

      bytes = new byte[6];
      ret = m2.readBytes(bytes);
      assertEquals(6, ret);
      assertByteArraysEqual(myBytes, bytes);

      ret = m2.readBytes(bytes);
      assertEquals(-1, ret);

      // Try and read past the end of the stream
      try
      {
         m2.readBoolean();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readByte();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readChar();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readDouble();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readFloat();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readInt();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readLong();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readShort();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readUnsignedByte();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readUnsignedShort();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      try
      {
         m2.readUTF();
         fail();
      }
      catch (MessageEOFException e)
      {
         // OK
      }

      // Message should not be writable in read-only mode
      try
      {
         m2.writeBoolean(myBool);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeByte(myByte);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeShort(myShort);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeChar(myChar);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }

      try
      {
         m2.writeInt(myInt);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeLong(myLong);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeFloat(myFloat);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeDouble(myDouble);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }
      try
      {
         m2.writeUTF(myString);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }

      try
      {
         m2.writeBytes(myBytes);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }

      try
      {
         m2.writeObject(myString);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
         // OK
      }

      long bodyLength = m2.getBodyLength();

      assertEquals(161, bodyLength);

      m2.reset();

      // test the unsigned reads

      m2.readBoolean();
      int unsignedByte = m2.readUnsignedByte();

      assertEquals((int)(myByte & 0xFF), unsignedByte);

      int unsignedShort = m2.readUnsignedShort();

      assertEquals((int)(myShort & 0xFFFF), unsignedShort);

      m2.clearBody();

      try
      {
         m2.getBodyLength();
         fail();
      }
      catch (MessageNotReadableException e)
      {
         // OK
      }

      m2.reset();

      assertEquals(0, m2.getBodyLength());

      // Test that changing the received message doesn't affect the sent message
      m.reset();
      assertEquals(161, m.getBodyLength());

      // Should be diffent object instances after sending *even* if in same JVM
      assertFalse(m == m2);

   }

   public void testMapMessage() throws Exception
   {
      MapMessage m1 = queueProducerSession.createMapMessage();

      // Some arbitrary values
      boolean myBool = true;
      byte myByte = 13;
      short myShort = 15321;
      int myInt = 0x71ab6c80;
      long myLong = 0x20bf1e3fb6fa31dfL;
      float myFloat = Float.MAX_VALUE - 23465;
      double myDouble = Double.MAX_VALUE - 72387633;
      String myString = "abcdef&^*&!^ghijkl\uD5E2\uCAC7\uD2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

      m1.setBoolean("myBool", myBool);
      m1.setByte("myByte", myByte);
      m1.setShort("myShort", myShort);
      m1.setInt("myInt", myInt);
      m1.setLong("myLong", myLong);
      m1.setFloat("myFloat", myFloat);
      m1.setDouble("myDouble", myDouble);
      m1.setString("myString", myString);

      m1.setObject("myBool", new Boolean(myBool));
      m1.setObject("myByte", new Byte(myByte));
      m1.setObject("myShort", new Short(myShort));
      m1.setObject("myInt", new Integer(myInt));
      m1.setObject("myLong", new Long(myLong));
      m1.setObject("myFloat", new Float(myFloat));
      m1.setObject("myDouble", new Double(myDouble));
      m1.setObject("myString", myString);

      try
      {
         m1.setObject("myIllegal", new Object());
         fail();
      }
      catch (javax.jms.MessageFormatException e)
      {
      }

      queueProducer.send(queue1, m1);

      MapMessage m2 = (MapMessage)queueConsumer.receive(2000);

      assertNotNull(m2);

      assertEquals(myBool, m2.getBoolean("myBool"));
      assertEquals(myByte, m2.getByte("myByte"));
      assertEquals(myShort, m2.getShort("myShort"));
      assertEquals(myInt, m2.getInt("myInt"));
      assertEquals(myLong, m2.getLong("myLong"));
      assertEquals(myFloat, m2.getFloat("myFloat"), 0);
      assertEquals(myDouble, m2.getDouble("myDouble"), 0);
      assertEquals(myString, m2.getString("myString"));

      // Properties should now be read-only
      try
      {
         m2.setBoolean("myBool", myBool);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setByte("myByte", myByte);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setShort("myShort", myShort);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setInt("myInt", myInt);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setLong("myLong", myLong);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setFloat("myFloat", myFloat);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setDouble("myDouble", myDouble);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      try
      {
         m2.setString("myString", myString);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      assertTrue(m2.itemExists("myBool"));
      assertTrue(m2.itemExists("myByte"));
      assertTrue(m2.itemExists("myShort"));
      assertTrue(m2.itemExists("myInt"));
      assertTrue(m2.itemExists("myLong"));
      assertTrue(m2.itemExists("myFloat"));
      assertTrue(m2.itemExists("myDouble"));
      assertTrue(m2.itemExists("myString"));

      assertFalse(m2.itemExists("sausages"));

      HashSet itemNames = new HashSet();
      Enumeration en = m2.getMapNames();
      while (en.hasMoreElements())
      {
         String propName = (String)en.nextElement();
         itemNames.add(propName);
      }

      assertEquals(8, itemNames.size());

      assertTrue(itemNames.contains("myBool"));
      assertTrue(itemNames.contains("myByte"));
      assertTrue(itemNames.contains("myShort"));
      assertTrue(itemNames.contains("myInt"));
      assertTrue(itemNames.contains("myLong"));
      assertTrue(itemNames.contains("myFloat"));
      assertTrue(itemNames.contains("myDouble"));
      assertTrue(itemNames.contains("myString"));

      // Check property conversions

      // Boolean property can be read as String but not anything else

      assertEquals(String.valueOf(myBool), m2.getString("myBool"));

      try
      {
         m2.getByte("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShort("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getInt("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getLong("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDouble("myBool");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // byte item can be read as short, int, long or String

      assertEquals((short)myByte, m2.getShort("myByte"));
      assertEquals((int)myByte, m2.getInt("myByte"));
      assertEquals((long)myByte, m2.getLong("myByte"));
      assertEquals(String.valueOf(myByte), m2.getString("myByte"));

      try
      {
         m2.getBoolean("myByte");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myByte");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDouble("myByte");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // short item can be read as int, long or String

      assertEquals((int)myShort, m2.getInt("myShort"));
      assertEquals((long)myShort, m2.getLong("myShort"));
      assertEquals(String.valueOf(myShort), m2.getString("myShort"));

      try
      {
         m2.getByte("myShort");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBoolean("myShort");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myShort");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDouble("myShort");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // int item can be read as long or String

      assertEquals((long)myInt, m2.getLong("myInt"));
      assertEquals(String.valueOf(myInt), m2.getString("myInt"));

      try
      {
         m2.getShort("myInt");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByte("myInt");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBoolean("myInt");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myInt");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDouble("myInt");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // long item can be read as String

      assertEquals(String.valueOf(myLong), m2.getString("myLong"));

      try
      {
         m2.getInt("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShort("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByte("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBoolean("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getDouble("myLong");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // float can be read as double or String

      assertEquals(String.valueOf(myFloat), m2.getString("myFloat"));
      assertEquals((double)myFloat, m2.getDouble("myFloat"), 0);

      try
      {
         m2.getInt("myFloat");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShort("myFloat");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getLong("myFloat");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByte("myFloat");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBoolean("myFloat");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // double can be read as String

      assertEquals(String.valueOf(myDouble), m2.getString("myDouble"));

      try
      {
         m2.getFloat("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getInt("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getShort("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getByte("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getBoolean("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      try
      {
         m2.getFloat("myDouble");
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      m2.clearBody();

      assertFalse(m2.getMapNames().hasMoreElements());

      // Test String -> Numeric and bool conversions
      MapMessage m3 = (MapMessage)queueProducerSession.createMapMessage();

      m3.setString("myBool", String.valueOf(myBool));
      m3.setString("myByte", String.valueOf(myByte));
      m3.setString("myShort", String.valueOf(myShort));
      m3.setString("myInt", String.valueOf(myInt));
      m3.setString("myLong", String.valueOf(myLong));
      m3.setString("myFloat", String.valueOf(myFloat));
      m3.setString("myDouble", String.valueOf(myDouble));
      m3.setString("myIllegal", "xyz123");

      assertEquals(myBool, m3.getBoolean("myBool"));
      assertEquals(myByte, m3.getByte("myByte"));
      assertEquals(myShort, m3.getShort("myShort"));
      assertEquals(myInt, m3.getInt("myInt"));
      assertEquals(myLong, m3.getLong("myLong"));
      assertEquals(myFloat, m3.getFloat("myFloat"), 0);
      assertEquals(myDouble, m3.getDouble("myDouble"), 0);

      m3.getBoolean("myIllegal");

      try
      {
         m3.getByte("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m3.getShort("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m3.getInt("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m3.getLong("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m3.getFloat("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }
      try
      {
         m3.getDouble("myIllegal");
         fail();
      }
      catch (NumberFormatException e)
      {
      }

   }

   static class TestSerializable implements Serializable
   {
      private static final long serialVersionUID = -8641359255228705573L;

      String str;
   }

   public void testObjectMessage() throws Exception
   {
      TestSerializable obj = new TestSerializable();

      obj.str = "abcdefg";

      ObjectMessage m1 = queueProducerSession.createObjectMessage(obj);

      queueProducer.send(queue1, m1);

      ObjectMessage m2 = (ObjectMessage)queueConsumer.receive(2000);

      assertNotNull(m2);

      TestSerializable obj2 = (TestSerializable)m2.getObject();

      assertEquals(obj.str, obj2.str);

      ObjectMessage m3 = queueProducerSession.createObjectMessage();

      m3.setObject(obj);

      queueProducer.send(queue1, m3);

      obj.str = "xyz123";

      ObjectMessage m4 = (ObjectMessage)queueConsumer.receive(2000);

      assertNotNull(m4);

      TestSerializable obj3 = (TestSerializable)m4.getObject();

      assertEquals("abcdefg", obj3.str);

      try
      {
         m4.setObject(obj);
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      m4.clearBody();

      m4.setObject(obj);

      TestSerializable obj4 = (TestSerializable)m4.getObject();

      assertNotNull(obj4);

   }

   public void testStreamMessage() throws Exception
   {
      StreamMessage m = queueProducerSession.createStreamMessage();

      // Some arbitrary values
      boolean myBool = true;
      byte myByte = -111;
      short myShort = 15321;
      int myInt = 0x71ab6c80;
      long myLong = 0x20bf1e3fb6fa31dfL;
      float myFloat = Float.MAX_VALUE - 23465;
      double myDouble = Double.MAX_VALUE - 72387633;
      String myString = "abcdef&^*&!^ghijkl\uD5E2\uCAC7\uD2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";
      char myChar = 'q';
      byte[] myBytes = new byte[] { -23, 114, -126, -12, 74, 87 };

      m.writeBoolean(myBool);
      m.writeByte(myByte);
      m.writeShort(myShort);
      m.writeChar(myChar);
      m.writeInt(myInt);
      m.writeLong(myLong);
      m.writeFloat(myFloat);
      m.writeDouble(myDouble);
      m.writeString(myString);
      m.writeBytes(myBytes);
      m.writeBytes(myBytes, 2, 3);

      m.writeObject(new Boolean(myBool));
      m.writeObject(new Byte(myByte));
      m.writeObject(new Short(myShort));
      m.writeObject(new Integer(myInt));
      m.writeObject(new Long(myLong));
      m.writeObject(new Float(myFloat));
      m.writeObject(new Double(myDouble));
      m.writeObject(myString);
      m.writeObject(myBytes);

      try
      {
         m.writeObject(new Object());
         fail();
      }
      catch (MessageFormatException e)
      {
      }

      // Reading should not be possible when message is read-write
      try
      {
         m.readBoolean();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readShort();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readChar();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readInt();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readLong();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readFloat();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         m.readDouble();
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }
      try
      {
         byte[] bytes = new byte[333];
         m.readBytes(bytes);
         fail();
      }
      catch (javax.jms.MessageNotReadableException e)
      {
      }

      queueProducer.send(queue1, m);

      StreamMessage m2 = (StreamMessage)queueConsumer.receive(2000);

      assertEquals(myBool, m2.readBoolean());
      assertEquals(myByte, m2.readByte());
      assertEquals(myShort, m2.readShort());
      assertEquals(myChar, m2.readChar());
      assertEquals(myInt, m2.readInt());
      assertEquals(myLong, m2.readLong());
      assertEquals(myFloat, m2.readFloat(), 0);
      assertEquals(myDouble, m2.readDouble(), 0);
      assertEquals(myString, m2.readString());

      byte[] bytes = new byte[6];
      int ret = m2.readBytes(bytes);
      assertEquals(6, ret);

      assertByteArraysEqual(myBytes, bytes);

      ret = m2.readBytes(bytes);
      assertEquals(-1, ret);

      byte[] bytes2 = new byte[3];
      ret = m2.readBytes(bytes2);

      assertEquals(3, ret);

      assertEquals(myBytes[2], bytes2[0]);
      assertEquals(myBytes[3], bytes2[1]);
      assertEquals(myBytes[4], bytes2[2]);

      ret = m2.readBytes(bytes2);
      assertEquals(-1, ret);

      assertEquals(myBool, m2.readBoolean());
      assertEquals(myByte, m2.readByte());
      assertEquals(myShort, m2.readShort());
      assertEquals(myInt, m2.readInt());
      assertEquals(myLong, m2.readLong());
      assertEquals(myFloat, m2.readFloat(), 0);
      assertEquals(myDouble, m2.readDouble(), 0);
      assertEquals(myString, m2.readString());

      bytes = new byte[6];
      ret = m2.readBytes(bytes);
      assertEquals(6, ret);
      assertByteArraysEqual(myBytes, bytes);

      ret = m2.readBytes(bytes);
      assertEquals(-1, ret);

      // Try and read past the end of the stream
      try
      {
         m2.readBoolean();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readByte();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readChar();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readDouble();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readFloat();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readInt();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readLong();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      try
      {
         m2.readShort();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      // Message should not be writable in read-only mode
      try
      {
         m2.writeBoolean(myBool);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeByte(myByte);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeShort(myShort);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeChar(myChar);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }

      try
      {
         m2.writeInt(myInt);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeLong(myLong);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeFloat(myFloat);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }
      try
      {
         m2.writeDouble(myDouble);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }

      try
      {
         m2.writeBytes(myBytes);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }

      try
      {
         m2.writeObject(myString);
         fail();
      }
      catch (javax.jms.MessageNotWriteableException e)
      {
      }

      m2.reset();

      // check we go back to the beginning
      assertEquals(myBool, m2.readBoolean());
      assertEquals(myByte, m2.readByte());
      assertEquals(myShort, m2.readShort());
      assertEquals(myChar, m2.readChar());
      assertEquals(myInt, m2.readInt());
      assertEquals(myLong, m2.readLong());
      assertEquals(myFloat, m2.readFloat(), 0);
      assertEquals(myDouble, m2.readDouble(), 0);
      assertEquals(myString, m2.readString());

      m2.clearBody();

      try
      {
         // Should now be write only
         m2.readBoolean();
         fail();
      }
      catch (MessageNotReadableException e)
      {
      }

      m2.writeBoolean(myBool);

      m2.reset();

      assertEquals(myBool, m2.readBoolean());
      try
      {
         m2.readBoolean();
         fail();
      }
      catch (MessageEOFException e)
      {
      }

      // Test that changing the received message doesn't affect the sent message
      m.reset();
      assertEquals(myBool, m.readBoolean());
      assertEquals(myByte, m.readByte());
      assertEquals(myShort, m.readShort());
      assertEquals(myChar, m.readChar());
      assertEquals(myInt, m.readInt());
      assertEquals(myLong, m.readLong());
      assertEquals(myFloat, m.readFloat(), 0);
      assertEquals(myDouble, m.readDouble(), 0);
      assertEquals(myString, m.readString());

      // Should be diffent object instances after sending *even* if in same JVM
      assertFalse(m == m2);
   }

   public void testTextMessage() throws Exception
   {
      TextMessage m = queueProducerSession.createTextMessage();

      // Arbitrary string with some Chinese characters to make sure UTF encoding
      // is ok
      String myString = "wwiuhdiuwhdwuhdwuhduqwhdiuwhdiuhwed8u29837482787\uD5E2\uCAC7\uD2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

      m.setText(myString);

      queueProducer.send(queue1, m);

      TextMessage m2 = (TextMessage)queueConsumer.receive(2000);

      assertEquals(myString, m2.getText());

      m = queueProducerSession.createTextMessage(myString);
      queueProducer.send(queue1, m);

      m2 = (TextMessage)queueConsumer.receive(2000);

      assertEquals(myString, m2.getText());

      try
      {
         m2.setText("Should be read-only");
         fail();
      }
      catch (MessageNotWriteableException e)
      {
      }

      m2.clearBody();
      assertNull(m2.getText());
      m2.setText("Now it is read-write");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void assertByteArraysEqual(byte[] bytes1, byte[] bytes2)
   {
      if (bytes1 == null | bytes2 == null)
      {
         fail();
      }

      if (bytes1.length != bytes2.length)
      {
         fail();
      }

      for (int i = 0; i < bytes1.length; i++)
      {
         assertEquals(bytes1[i], bytes2[i]);
      }

   }

   // Inner classes -------------------------------------------------

}
