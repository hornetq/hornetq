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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * Testing of message property conversion. 
 * See javax.jms.Message for details
 * 
 * @author <a href="mailto:afu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessagePropertyConversionTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   private Destination queue;
   private Connection producerConnection, consumerConnection;
   private Session queueProducerSession, queueConsumerSession;
   private MessageProducer queueProducer;
   private MessageConsumer queueConsumer;
   
   // Constructors --------------------------------------------------

   public MessagePropertyConversionTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      queue = (Destination)ic.lookup("/queue/Queue");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue);
      queueConsumer = queueConsumerSession.createConsumer(queue);

      consumerConnection.start();
   }

   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();

      ServerManagement.undeployQueue("Queue");
      

      super.tearDown();
   }
   
   public void testBooleanConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      boolean myBool = true;
      m1.setBooleanProperty("myBool", myBool);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);
      
      // Boolean property can be read as boolean and String but not anything else

      assertEquals(myBool, m2.getBooleanProperty("myBool"));
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
   }
   
   public void testByteConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      byte myByte = 13;
      m1.setByteProperty("myByte", myByte);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);
      
      // Byte property can be read as byte, short, int, long or String

      assertEquals(myByte, m2.getByteProperty("myByte"));
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
   }
   
   public void testShortConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      short myShort = 15321;
      m1.setShortProperty("myShort", myShort);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);
 
      // Short property can be read as short, int, long or String

      assertEquals(myShort, m2.getShortProperty("myShort"));
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
   }
   
   public void testIntConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      int myInt = 0x71ab6c80;
      m1.setIntProperty("myInt", myInt);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);

      // Int property can be read as int, long or String

      assertEquals(myInt, m2.getIntProperty("myInt"));
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
   }
   
   public void testLongConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      long myLong = 0x20bf1e3fb6fa31dfL;
      m1.setLongProperty("myLong", myLong);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);

      // Long property can be read as long and String

      assertEquals(myLong, m2.getLongProperty("myLong"));
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
   }
   
   public void testFloatConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      float myFloat = Float.MAX_VALUE - 23465;
      m1.setFloatProperty("myFloat", myFloat);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);
   
      // Float property can be read as float, double or String

      assertEquals(myFloat, m2.getFloatProperty("myFloat"), 0);
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
   }
   
   public void testDoubleConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();

      double myDouble = Double.MAX_VALUE - 72387633;
      m1.setDoubleProperty("myDouble", myDouble);

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);

      // Double property can be read as double and String

      assertEquals(myDouble, m2.getDoubleProperty("myDouble"), 0);
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

      queueProducer.send(queue, m1);
      Message m2 = queueConsumer.receive(2000);

      assertEquals(myString, m2.getStringProperty("myString"));

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

      queueProducer.send(queue, m3);

      Message m4 = queueConsumer.receive(2000);

      assertEquals(myBool, m4.getBooleanProperty("myBool"));
      assertEquals(myByte, m4.getByteProperty("myByte"));
      assertEquals(myShort, m4.getShortProperty("myShort"));
      assertEquals(myInt, m4.getIntProperty("myInt"));
      assertEquals(myLong, m4.getLongProperty("myLong"));
      assertEquals(myFloat, m4.getFloatProperty("myFloat"), 0);
      assertEquals(myDouble, m4.getDoubleProperty("myDouble"), 0);

      assertEquals(false, m4.getBooleanProperty("myIllegal"));

      try
      {
         m4.getByteProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m4.getShortProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m4.getIntProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m4.getLongProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m4.getFloatProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
      try
      {
         m4.getDoubleProperty("myIllegal");
         fail();
      }
      catch (NumberFormatException e) {}
   }   
   
   public void testJMSXDeliveryCountConversion() throws Exception
   {
      Message m1 = queueProducerSession.createMessage();
      queueProducer.send(queue, m1);

      Message m2 = queueConsumer.receive(2000);

      int count = m2.getIntProperty("JMSXDeliveryCount");
      assertEquals(String.valueOf(count), m2.getStringProperty("JMSXDeliveryCount"));
      assertEquals((long)count, m2.getLongProperty("JMSXDeliveryCount"));
   }
}
