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

import java.io.File;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashSet;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.SendAcknowledgementHandler;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.jms.client.HornetQMapMessage;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQObjectMessage;
import org.hornetq.jms.client.HornetQStreamMessage;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.utils.SimpleString;

/**
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

    // Public --------------------------------------------------------


   public void testClearMessage() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      Message message = queueProducerSession.createTextMessage("some message");

      queueProducer.send(message);

      message = queueConsumer.receive(1000);

      assertNotNull(message);

      message.clearProperties();

      assertNotNull(message.getJMSDestination());

   }

   public void testMessageOrderQueue() throws Exception
   {
      final int NUM_MESSAGES = 10;
      
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = queueProducerSession.createMessage();
         m.setIntProperty("count", i);
         queueProducer.send(m);
      }
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = queueConsumer.receive(3000);
         assertNotNull(m);
         int count = m.getIntProperty("count");
         assertEquals(i, count);
      }
      
      queueProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = queueProducerSession.createMessage();
         m.setIntProperty("count2", i);
         queueProducer.send(m);
      }
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = queueConsumer.receive(3000);
         assertNotNull(m);
         int count = m.getIntProperty("count2");
         assertEquals(i, count);
      }
   }
   
   public void testMessageOrderTopic() throws Exception
   {
      final int NUM_MESSAGES = 10;
      
      topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = topicProducerSession.createMessage();
         m.setIntProperty("count", i);
         topicProducer.send(m);
      }
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = topicConsumer.receive(3000);
         assertNotNull(m);
         int count = m.getIntProperty("count");
         assertEquals(i, count);
      }
      
      topicProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = topicProducerSession.createMessage();
         m.setIntProperty("count2", i);
         topicProducer.send(m);
      }
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = topicConsumer.receive(3000);
         assertNotNull(m);
         int count = m.getIntProperty("count2");
         assertEquals(i, count);
      }
   }
   

   public void testProperties() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      //Some arbitrary values
      boolean myBool = true;
      byte myByte = 13;
      short myShort = 15321;
      int myInt = 0x71ab6c80;
      long myLong = 0x20bf1e3fb6fa31dfL;
      float myFloat = Float.MAX_VALUE - 23465;
      double myDouble = Double.MAX_VALUE - 72387633;
      String myString = "abcdef&^*&!^ghijkl";

      m1.setBooleanProperty("myBool", myBool);
      m1.setByteProperty("myByte", myByte);
      m1.setShortProperty("myShort", myShort);
      m1.setIntProperty("myInt", myInt);
      m1.setLongProperty("myLong", myLong);
      m1.setFloatProperty("myFloat", myFloat);
      m1.setDoubleProperty("myDouble", myDouble);
      m1.setStringProperty("myString", myString);

      m1.setObjectProperty("myBool", new Boolean(myBool));
      m1.setObjectProperty("myByte", new Byte(myByte));
      m1.setObjectProperty("myShort", new Short(myShort));
      m1.setObjectProperty("myInt", new Integer(myInt));
      m1.setObjectProperty("myLong", new Long(myLong));
      m1.setObjectProperty("myFloat", new Float(myFloat));
      m1.setObjectProperty("myDouble", new Double(myDouble));
      m1.setObjectProperty("myString", myString);

      try
      {
         m1.setObjectProperty("myIllegal", new Object());
         fail();
      }
      catch (javax.jms.MessageFormatException e)
      {}


      queueProducer.send(queue1, m1);

      Message m2 = queueConsumer.receive(2000);

      assertNotNull(m2);

      assertEquals(myBool, m2.getBooleanProperty("myBool"));
      assertEquals(myByte, m2.getByteProperty("myByte"));
      assertEquals(myShort, m2.getShortProperty("myShort"));
      assertEquals(myInt, m2.getIntProperty("myInt"));
      assertEquals(myLong, m2.getLongProperty("myLong"));
      assertEquals(myFloat, m2.getFloatProperty("myFloat"), 0);
      assertEquals(myDouble, m2.getDoubleProperty("myDouble"), 0);
      assertEquals(myString, m2.getStringProperty("myString"));


      //Properties should now be read-only
      try
      {
         m2.setBooleanProperty("myBool", myBool);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setByteProperty("myByte", myByte);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setShortProperty("myShort", myShort);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setIntProperty("myInt", myInt);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setLongProperty("myLong", myLong);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setFloatProperty("myFloat", myFloat);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setDoubleProperty("myDouble", myDouble);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      try
      {
         m2.setStringProperty("myString", myString);
         fail();
      }
      catch (MessageNotWriteableException e) {}

      assertTrue(m2.propertyExists("myBool"));
      assertTrue(m2.propertyExists("myByte"));
      assertTrue(m2.propertyExists("myShort"));
      assertTrue(m2.propertyExists("myInt"));
      assertTrue(m2.propertyExists("myLong"));
      assertTrue(m2.propertyExists("myFloat"));
      assertTrue(m2.propertyExists("myDouble"));
      assertTrue(m2.propertyExists("myString"));

      assertFalse(m2.propertyExists("sausages"));

      HashSet propNames = new HashSet();
      Enumeration en = m2.getPropertyNames();
      while (en.hasMoreElements())
      {
         String propName = (String)en.nextElement();
         
         propNames.add(propName);
      }

      assertEquals(9, propNames.size());

      assertTrue(propNames.contains("myBool"));
      assertTrue(propNames.contains("myByte"));
      assertTrue(propNames.contains("myShort"));
      assertTrue(propNames.contains("myInt"));
      assertTrue(propNames.contains("myLong"));
      assertTrue(propNames.contains("myFloat"));
      assertTrue(propNames.contains("myDouble"));
      assertTrue(propNames.contains("myString"));


      // Check property conversions

      //Boolean property can be read as String but not anything else

      assertEquals(String.valueOf(myBool), m2.getStringProperty("myBool"));

      try
      {
         m2.getByteProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getShortProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getIntProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getLongProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getDoubleProperty("myBool");
         fail();
      } catch (MessageFormatException e) {}


      // byte property can be read as short, int, long or String

      assertEquals((short)myByte, m2.getShortProperty("myByte"));
      assertEquals((int)myByte, m2.getIntProperty("myByte"));
      assertEquals((long)myByte, m2.getLongProperty("myByte"));
      assertEquals(String.valueOf(myByte), m2.getStringProperty("myByte"));

      try
      {
         m2.getBooleanProperty("myByte");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myByte");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getDoubleProperty("myByte");
         fail();
      } catch (MessageFormatException e) {}


      // short property can be read as int, long or String

      assertEquals((int)myShort, m2.getIntProperty("myShort"));
      assertEquals((long)myShort, m2.getLongProperty("myShort"));
      assertEquals(String.valueOf(myShort), m2.getStringProperty("myShort"));

      try
      {
         m2.getByteProperty("myShort");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getBooleanProperty("myShort");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myShort");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getDoubleProperty("myShort");
         fail();
      } catch (MessageFormatException e) {}

      // int property can be read as long or String

      assertEquals((long)myInt, m2.getLongProperty("myInt"));
      assertEquals(String.valueOf(myInt), m2.getStringProperty("myInt"));

      try
      {
         m2.getShortProperty("myInt");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getByteProperty("myInt");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getBooleanProperty("myInt");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myInt");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getDoubleProperty("myInt");
         fail();
      } catch (MessageFormatException e) {}


      // long property can be read as String

      assertEquals(String.valueOf(myLong), m2.getStringProperty("myLong"));

      try
      {
         m2.getIntProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getShortProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getByteProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getBooleanProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getDoubleProperty("myLong");
         fail();
      } catch (MessageFormatException e) {}


      // float property can be read as double or String

      assertEquals(String.valueOf(myFloat), m2.getStringProperty("myFloat"));
      assertEquals((double)myFloat, m2.getDoubleProperty("myFloat"), 0);

      try
      {
         m2.getIntProperty("myFloat");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getShortProperty("myFloat");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getLongProperty("myFloat");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getByteProperty("myFloat");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getBooleanProperty("myFloat");
         fail();
      } catch (MessageFormatException e) {}



      // double property can be read as String

      assertEquals(String.valueOf(myDouble), m2.getStringProperty("myDouble"));


      try
      {
         m2.getFloatProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getIntProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getShortProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getByteProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getBooleanProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      try
      {
         m2.getFloatProperty("myDouble");
         fail();
      } catch (MessageFormatException e) {}

      m2.clearProperties();

      Enumeration en2 = m2.getPropertyNames();
      assertTrue(en2.hasMoreElements());
      en2.nextElement();
      assertFalse(en2.hasMoreElements());




      // Test String -> Numeric and bool conversions
      Message m3 = queueProducerSession.createMessage();

      m3.setStringProperty("myBool", String.valueOf(myBool));
      m3.setStringProperty("myByte", String.valueOf(myByte));
      m3.setStringProperty("myShort", String.valueOf(myShort));
      m3.setStringProperty("myInt", String.valueOf(myInt));
      m3.setStringProperty("myLong", String.valueOf(myLong));
      m3.setStringProperty("myFloat", String.valueOf(myFloat));
      m3.setStringProperty("myDouble", String.valueOf(myDouble));
      m3.setStringProperty("myIllegal", "xyz123");

      assertEquals(myBool, m3.getBooleanProperty("myBool"));
      assertEquals(myByte, m3.getByteProperty("myByte"));
      assertEquals(myShort, m3.getShortProperty("myShort"));
      assertEquals(myInt, m3.getIntProperty("myInt"));
      assertEquals(myLong, m3.getLongProperty("myLong"));
      assertEquals(myFloat, m3.getFloatProperty("myFloat"), 0);
      assertEquals(myDouble, m3.getDoubleProperty("myDouble"), 0);

      m3.getBooleanProperty("myIllegal");

      try
      {
         m3.getByteProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m3.getShortProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m3.getIntProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m3.getLongProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m3.getFloatProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m3.getDoubleProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
   }



   public void testSendReceiveForeignMessage() throws JMSException
   {
      
      log.trace("Starting da test");
      
      SimpleJMSMessage foreignMessage = new SimpleJMSMessage();
      
      foreignMessage.setStringProperty("animal", "aardvark");
      
      //foreign messages don't have to be serializable
      assertFalse(foreignMessage instanceof Serializable);
      
      log.trace("Sending message");
      
      queueProducer.send(foreignMessage);
      
      log.trace("Sent message");

      Message m2 = queueConsumer.receive(3000);
      log.trace("The message is " + m2);
      
      assertNotNull(m2);
      
      assertEquals("aardvark", m2.getStringProperty("animal"));
      
      log.trace("Received message");

      log.trace("Done that test");
   }

   public void testCopyOnJBossMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);
      HornetQMessage jbossMessage = HornetQMessage.createMessage(clientMessage, session);
      jbossMessage.clearProperties();

      configureMessage(jbossMessage);

      HornetQMessage copy = new HornetQMessage(jbossMessage, session);

      ensureEquivalent(jbossMessage, copy);
   }


   public void testCopyOnForeignMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);

      Message foreignMessage = new SimpleJMSMessage();

      HornetQMessage copy = new HornetQMessage(foreignMessage, session);

      ensureEquivalent(foreignMessage, copy);

   }
   
   public void testCopyOnForeignBytesMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);

      BytesMessage foreignBytesMessage = new SimpleJMSBytesMessage();
      for(int i = 0; i < 20; i++)
      {
         foreignBytesMessage.writeByte((byte)i);
      }

      HornetQBytesMessage copy = new HornetQBytesMessage(foreignBytesMessage, session);

      foreignBytesMessage.reset();
      copy.reset();

      ensureEquivalent(foreignBytesMessage, copy);
   }
  
   public void testCopyOnForeignMapMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);
      MapMessage foreignMapMessage = new SimpleJMSMapMessage();
      foreignMapMessage.setInt("int", 1);
      foreignMapMessage.setString("string", "test");

      HornetQMapMessage copy = new HornetQMapMessage(foreignMapMessage, session);

      ensureEquivalent(foreignMapMessage, copy);
   }


   public void testCopyOnForeignObjectMessage() throws JMSException
   {      
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);

      ObjectMessage foreignObjectMessage = new SimpleJMSObjectMessage();

      HornetQObjectMessage copy = new HornetQObjectMessage(foreignObjectMessage, session);

      ensureEquivalent(foreignObjectMessage, copy);
   }


   public void testCopyOnForeignStreamMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);

      StreamMessage foreignStreamMessage = new SimpleJMSStreamMessage();
      foreignStreamMessage.writeByte((byte)1);
      foreignStreamMessage.writeByte((byte)2);
      foreignStreamMessage.writeByte((byte)3);

      HornetQStreamMessage copy = new HornetQStreamMessage(foreignStreamMessage, session);

      ensureEquivalent(foreignStreamMessage, copy);
   }


   public void testCopyOnForeignTextMessage() throws JMSException
   {
      ClientMessage clientMessage = new ClientMessageImpl(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, 1000);
      ClientSession session = new FakeSession(clientMessage);
      TextMessage foreignTextMessage = new SimpleJMSTextMessage();

      HornetQTextMessage copy = new HornetQTextMessage(foreignTextMessage, session);

      ensureEquivalent(foreignTextMessage, copy);
   }
   
   public void testForeignJMSDestination() throws JMSException
   {
      Message message = queueProducerSession.createMessage();
      
      Destination foreignDestination = new ForeignDestination();
      
      message.setJMSDestination(foreignDestination);
      
      assertSame(foreignDestination, message.getJMSDestination());
      
      queueProducer.send(message);
      
      assertSame(queue1, message.getJMSDestination());
      
      Message receivedMessage = queueConsumer.receive(2000);
      
      ensureEquivalent(receivedMessage, (HornetQMessage)message);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class ForeignDestination implements Destination, Serializable
   {
		private static final long serialVersionUID = 5545509674580823610L;

		// A ForeignDestination equals any other ForeignDestination, for simplicity
      public boolean equals(Object obj)
      {
         return obj instanceof ForeignDestination;
      }
      
      public int hashCode()
      {
         return 157;
      }
   }

   class FakeSession implements ClientSession
   {
      public ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws HornetQException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public ClientConsumer createConsumer(String queueName, boolean browseOnly) throws HornetQException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void createQueue(String address, String queueName) throws HornetQException
      {
         // TODO Auto-generated method stub
         
      }

      private final ClientMessage message;

      public FakeSession(ClientMessage message)
      {
         this.message = message;
      }

      public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws HornetQException
      {
      }
      
      public void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException
      {
      }

      public void createQueue(String address, String queueName, boolean durable) throws HornetQException
      {
      }

      public void createQueue(SimpleString address, SimpleString queueName, boolean durable, boolean temporary) throws HornetQException
      {
      }

      public void createQueue(String address, String queueName, boolean durable, boolean temporary) throws HornetQException
      {
      }

      public void createQueue(String address, String queueName, String filterString, boolean durable) throws HornetQException
      {
      }

      public void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException
      {
      }

      public void createTemporaryQueue(String address, String queueName) throws HornetQException
      {
      }

      public void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException
      {
      }

      public void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException
      {
      }

      public void deleteQueue(SimpleString queueName) throws HornetQException
      {
      }

      public void deleteQueue(String queueName) throws HornetQException
      {
      }

      public ClientConsumer createConsumer(SimpleString queueName) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, int windowSize, int maxRate, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(String queueName) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(String queueName, String filterString) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createConsumer(String queueName, String filterString, int windowSize, int maxRate, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, SimpleString queueName) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, SimpleString queueName, SimpleString filterString) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, SimpleString queueName, SimpleString filterString, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, SimpleString queueName, SimpleString filterString, int windowSize, int maxRate, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, String queueName) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, String queueName, String filterString) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, String queueName, String filterString, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientConsumer createFileConsumer(File directory, String queueName, String filterString, int windowSize, int maxRate, boolean browseOnly) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer() throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(SimpleString address) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(SimpleString address, int rate) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(SimpleString address, int maxRate, boolean blockOnNonPersistentSend, boolean blockOnPersistentSend) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(String address) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(String address, int rate) throws HornetQException
      {
         return null;
      }

      public ClientProducer createProducer(String address, int maxRate, boolean blockOnNonPersistentSend, boolean blockOnPersistentSend) throws HornetQException
      {
         return null;
      }

      public SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws HornetQException
      {
         return null;
      }

      public SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws HornetQException
      {
         return null;
      }

      public XAResource getXAResource()
      {
         return null;
      }

      public void commit() throws HornetQException
      {
      }

      public boolean isRollbackOnly()
      {

         return false;
      }

      public void rollback() throws HornetQException
      {
      }

      public void rollback(boolean considerLastMessageAsDelivered) throws HornetQException
      {
      }

      public void close() throws HornetQException
      {
      }

      public boolean isClosed()
      {
         return false;
      }

      public boolean isAutoCommitSends()
      {
         return false;
      }

      public boolean isAutoCommitAcks()
      {
         return false;
      }

      public boolean isBlockOnAcknowledge()
      {
         return false;
      }

      public boolean isXA()
      {
         return false;
      }

      public ClientMessage createClientMessage(byte type, boolean durable, long expiration, long timestamp, byte priority)
      {
         return message;
      }

      public ClientMessage createClientMessage(byte type, boolean durable)
      {
         return message;
      }

      public ClientMessage createClientMessage(boolean durable)
      {
         return message;
      }

      public void start() throws HornetQException
      {
      }

      public void stop() throws HornetQException
      {
      }

      public void addFailureListener(FailureListener listener)
      {
      }

      public boolean removeFailureListener(FailureListener listener)
      {
         return false;
      }

      public int getVersion()
      {
         return 0;
      }

      public void setSendAcknowledgementHandler(SendAcknowledgementHandler handler)
      {
      }

      public void commit(Xid xid, boolean b) throws XAException
      {
      }

      public void end(Xid xid, int i) throws XAException
      {
      }

      public void forget(Xid xid) throws XAException
      {
      }

      public int getTransactionTimeout() throws XAException
      {
         return 0;
      }

      public boolean isSameRM(XAResource xaResource) throws XAException
      {
         return false;
      }

      public int prepare(Xid xid) throws XAException
      {
         return 0;
      }

      public Xid[] recover(int i) throws XAException
      {
         return new Xid[0];
      }

      public void rollback(Xid xid) throws XAException
      {
      }

      public boolean setTransactionTimeout(int i) throws XAException
      {
         return false;
      }

      public void start(Xid xid, int i) throws XAException
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.client.ClientSession#createBuffer(byte[])
       */
      public HornetQBuffer createBuffer(byte[] bytes)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.client.ClientSession#createBuffer(int)
       */
      public HornetQBuffer createBuffer(int size)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.client.ClientSession#createClientMessage(boolean, org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public ClientMessage createClientMessage(boolean durable, HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void addFailureListener(SessionFailureListener listener)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean removeFailureListener(SessionFailureListener listener)
      {
         // TODO Auto-generated method stub
         return false;
      }
   }
   
}
