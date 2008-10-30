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

package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomShort;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.nio.ByteBuffer;
import java.util.Collections;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.client.JBossBytesMessage;
import org.jboss.messaging.jms.client.JBossMapMessage;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossObjectMessage;
import org.jboss.messaging.jms.client.JBossStreamMessage;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossMessageTest extends TestCase
{
   private ClientSession clientSession;
   private ClientMessage clientMessage;
   private String propertyName;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testCreateMessage() throws Exception
   {
      doTestCreateMessage(JBossMessage.TYPE, JBossMessage.class);
   }

   public void testCreateBytesMessage() throws Exception
   {
      doTestCreateMessage(JBossBytesMessage.TYPE, JBossBytesMessage.class);
   }

   public void testCreateMapMessage() throws Exception
   {
      doTestCreateMessage(JBossMapMessage.TYPE, JBossMapMessage.class);
   }

   public void testCreateObjectMessage() throws Exception
   {
      doTestCreateMessage(JBossObjectMessage.TYPE, JBossObjectMessage.class);
   }

   public void testCreateStreamMessage() throws Exception
   {
      doTestCreateMessage(JBossStreamMessage.TYPE, JBossStreamMessage.class);
   }

   public void testCreateTextMessage() throws Exception
   {
      doTestCreateMessage(JBossTextMessage.TYPE, JBossTextMessage.class);
   }

   public void testCreateInvalidMessage() throws Exception
   {
      try
      {
         doTestCreateMessage((byte) 23, JBossTextMessage.class);
         fail("must throw a IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testForeignMessage() throws Exception
   {
      Message foreignMessage = createNiceMock(Message.class);
      ClientSession session = EasyMock.createNiceMock(ClientSession.class);
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      ClientMessage clientMessage = new ClientMessageImpl(JBossMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      expect(session.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);
      expect(foreignMessage.getJMSDeliveryMode()).andReturn(
            DeliveryMode.NON_PERSISTENT);
      expect(foreignMessage.getPropertyNames()).andReturn(
            Collections.enumeration(Collections.EMPTY_LIST));

      replay(foreignMessage, session);

      JBossMessage msg = new JBossMessage(foreignMessage, session);

      verify(foreignMessage, session);
   }

   public void testGetJMSMessageID() throws Exception
   {
      SimpleString messageID = randomSimpleString();
      clientSession = createStrictMock(ClientSession.class);
      clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      expect(clientMessage.getProperty(JBossMessage.JBM_MESSAGE_ID)).andReturn(
            messageID);
      replay(clientSession, clientMessage, buffer);

      JBossMessage message = new JBossMessage(clientMessage, clientSession);
      assertEquals(messageID.toString(), message.getJMSMessageID());

      verify(clientSession, clientMessage, buffer);
   }
   
   public void testGetJMSDeliveryModeAsPersistent() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDurable(true);
      
      assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
   }

   public void testGetJMSDeliveryModeAsNonPersistent() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDurable(false);
      
      assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
   }

   public void testSetJMSDeliveryModeWithPersistent() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
      
      assertTrue(message.getCoreMessage().isDurable());
   }

   public void testSetJMSDeliveryModeWithNonPersistent() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      assertFalse(message.getCoreMessage().isDurable());
   }

   public void testSetJMSDeliveryModeWithInvalidValue() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setJMSDeliveryMode(12345);         
         fail("invalid DeliveryMode value");
      } catch (JMSException e)
      {
      }
   }
   
   public void testGetJMSRedelivered() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      message.getCoreMessage().setDeliveryCount(0);
      assertFalse(message.getJMSRedelivered());
      
      message.getCoreMessage().setDeliveryCount(1);
      assertFalse(message.getJMSRedelivered());

      message.getCoreMessage().setDeliveryCount(2);
      assertTrue(message.getJMSRedelivered());
   }
   
   public void testSetJMSRedeliveredToTrue() throws Exception
   {
      JBossMessage message = new JBossMessage();

      message.setJMSRedelivered(true);
      assertEquals(2, message.getCoreMessage().getDeliveryCount());
   }
   
   public void testSetJMSRedeliveredToFalse() throws Exception
   {
      JBossMessage message = new JBossMessage();

      message.setJMSRedelivered(false);
      assertEquals(1, message.getCoreMessage().getDeliveryCount());
   }

   public void testSetJMSType() throws Exception
   {
      String type = randomString();
      JBossMessage message = new JBossMessage();      
      message.setJMSType(type);
      
      SimpleString t = (SimpleString) message.getCoreMessage().getProperty(JBossMessage.TYPE_HEADER_NAME);
      assertEquals(type, t.toString());
   }
   
   public void testGetJMSType() throws Exception
   {
      String type = randomString();
      JBossMessage message = new JBossMessage();      
    
      assertNull(message.getJMSType());
      
      message.getCoreMessage().putStringProperty(JBossMessage.TYPE_HEADER_NAME, new SimpleString(type));
      assertEquals(type, message.getJMSType());
   }
   
   public void testSetJMSTimestamp() throws Exception
   {
      long timestamp = randomLong();
      JBossMessage message = new JBossMessage();      
      message.setJMSTimestamp(timestamp);
      
      assertEquals(timestamp, message.getCoreMessage().getTimestamp());
   }
   
   public void testGetJMSTimestamp() throws Exception
   {
      long timestamp = randomLong();
      JBossMessage message = new JBossMessage();      
      message.getCoreMessage().setTimestamp(timestamp);
    
      assertEquals(timestamp, message.getJMSTimestamp());
   }
   
   public void testSetJMSCorrelationID() throws Exception
   {
      String correlationID = randomString();
      JBossMessage message = new JBossMessage();      
    
      message.setJMSCorrelationID(correlationID);
      
      SimpleString value = (SimpleString) message.getCoreMessage().getProperty(JBossMessage.CORRELATIONID_HEADER_NAME);
      
      assertEquals(correlationID, value.toString());      
   }
   
   public void testSetJMSCorrelationIDToNull() throws Exception
   {
      JBossMessage message = new JBossMessage();      
      message.setJMSCorrelationID(null);
      
      assertFalse(message.getCoreMessage().containsProperty(JBossMessage.CORRELATIONID_HEADER_NAME));
   }
   
   public void testGetJMSCorrelationID() throws Exception
   {
      String correlationID = randomString();
      JBossMessage message = new JBossMessage();
      
      message.getCoreMessage().putStringProperty(JBossMessage.CORRELATIONID_HEADER_NAME, new SimpleString(correlationID));
      
      assertEquals(correlationID, message.getJMSCorrelationID());
   }
   
   
   public void testSetJMSCorrelationIDAsBytes() throws Exception
   {
      byte[] correlationID = randomBytes();
      JBossMessage message = new JBossMessage();      
    
      message.setJMSCorrelationIDAsBytes(correlationID);
      
      byte[] value = (byte[]) message.getCoreMessage().getProperty(JBossMessage.CORRELATIONID_HEADER_NAME);
      
      assertEquals(correlationID, value);      
   }
   
   public void testGetJMSCorrelationIDAsBytes() throws Exception
   {
      byte[] correlationID = randomBytes();
      JBossMessage message = new JBossMessage();
      
      message.getCoreMessage().putBytesProperty(JBossMessage.CORRELATIONID_HEADER_NAME, correlationID);
      
      assertEquals(correlationID, message.getJMSCorrelationIDAsBytes());
   }
   
   public void testSetPriority() throws Exception
   {
      int priority = 9;
      JBossMessage message = new JBossMessage();
      
      message.setJMSPriority(priority);
      assertEquals(priority, message.getCoreMessage().getPriority());
   }
   
   public void testSetInvalidPriority() throws Exception
   {
      int invalidPriority = 10;
      JBossMessage message = new JBossMessage();

      try
      {
         message.setJMSPriority(invalidPriority);
         fail("0 <= priority <= 9");
      }
      catch(JMSException e)
      {
      }
   }
   
   public void testGetPriority() throws Exception
   {
      int priority = 9;
      JBossMessage message = new JBossMessage();

      message.getCoreMessage().setPriority((byte) priority);
      assertEquals(priority, message.getJMSPriority());
   }
   
   public void testSetJMSExpiration() throws Exception
   {
      long expiration = randomLong();
      JBossMessage message = new JBossMessage();
      message.setJMSExpiration(expiration);
      
      assertEquals(expiration, message.getCoreMessage().getExpiration());
   }

   public void testGetJMSExpiration() throws Exception
   {
      long expiration = randomLong();
      JBossMessage message = new JBossMessage();
      
      message.getCoreMessage().setExpiration(expiration);
      assertEquals(expiration, message.getJMSExpiration());
   }

   public void testJMSXGroupIDPropertyNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      assertFalse(message.propertyExists(JBossMessage.JMSXGROUPID));
      assertNull(message.getStringProperty(JBossMessage.JMSXGROUPID));
   }

   public void testSetJMSXGroupIDProperty() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.setStringProperty(JBossMessage.JMSXGROUPID, "testid");
      assertTrue(message.propertyExists(JBossMessage.JMSXGROUPID));
      assertEquals(new SimpleString("testid"), message.getCoreMessage().getProperty(MessageImpl.GROUP_ID));
   }

   public void testGetJMSXGroupIDProperty() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(MessageImpl.GROUP_ID, new SimpleString("testid"));
      assertTrue(message.propertyExists(JBossMessage.JMSXGROUPID));
      assertEquals("testid", message.getStringProperty(JBossMessage.JMSXGROUPID));
   }

   public void testSetJMSMessageID() throws Exception
   {
      String messageID = "ID:" + randomString();
      clientSession = createStrictMock(ClientSession.class);
      clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      clientMessage.putStringProperty(JBossMessage.JBM_MESSAGE_ID,
            new SimpleString(messageID));
      replay(clientSession, clientMessage, buffer);

      JBossMessage message = new JBossMessage(clientMessage, clientSession);
      message.setJMSMessageID(messageID);

      verify(clientSession, clientMessage, buffer);
   }

   public void testSetJMSMessageIDNotStartingWithID() throws Exception
   {
      String messageID = randomString();
      assertTrue(!messageID.startsWith("ID:"));
      clientSession = createStrictMock(ClientSession.class);
      clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      replay(clientSession, clientMessage, buffer);

      JBossMessage message = new JBossMessage(clientMessage, clientSession);
      try
      {
         message.setJMSMessageID(messageID);
         fail("messageID does not start with ID:");
      }
      catch (JMSException e)
      {

      }

      verify(clientSession, clientMessage, buffer);
   }

   public void testSetJMSMessageIDWithNull() throws Exception
   {
      String messageID = null;
      clientSession = createStrictMock(ClientSession.class);
      clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      expect(clientMessage.removeProperty(JBossMessage.JBM_MESSAGE_ID))
            .andReturn(null);
      replay(clientSession, clientMessage, buffer);

      JBossMessage message = new JBossMessage(clientMessage, clientSession);
      message.setJMSMessageID(messageID);

      verify(clientSession, clientMessage, buffer);
   }

   public void testCheckPropertyNameIsNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setBooleanProperty(null, true);
         fail("property name can not be null");
      }
      catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testCheckPropertyNameIsEmpty() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setBooleanProperty("a-1", true);
         fail("property name can not be an invalid java identifier");
      } catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testCheckPropertyNameIsNotJavaIdentifier() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setBooleanProperty("", true);
         fail("property name can not be empty");
      }
      catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testCheckPropertyNameIsReservedIdentifier() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setBooleanProperty("BETWEEN", true);
         fail("property name can not be a reserverd identifier used by message selector");
      } catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testCheckPropertyNameStartingWithJMS() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.setBooleanProperty("JMSWathever", true);
         fail("property name can not start with JMS");
      } catch (IllegalArgumentException e)
      {
      }
   }
   public void testSetBooleanProperty() throws Exception
   {
      boolean value = true;

      JBossMessage message = new JBossMessage();
      message.setBooleanProperty(propertyName, value);

      boolean v = (Boolean) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testGetBooleanProperty() throws Exception
   {
      boolean value = true;
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putBooleanProperty(new SimpleString(propertyName), value);
      
      boolean v = message.getBooleanProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetBooleanPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      boolean v = message.getBooleanProperty(propertyName);
      assertEquals(false, v);
   }
   
   public void testGetBooleanPropertyWithString() throws Exception
   {
      SimpleString value = new SimpleString("true");
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      boolean v = message.getBooleanProperty(propertyName);
      assertEquals(true, v);
   }
   
   public void testGetBooleanPropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), randomFloat());

      try
      {
         message.getBooleanProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testSetByteProperty() throws Exception
   {
      byte value = randomByte();

      JBossMessage message = new JBossMessage();
      message.setByteProperty(propertyName, value);

      byte v = (Byte) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }
   
   public void testGetByteProperty() throws Exception
   {
      byte value = randomByte();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putByteProperty(new SimpleString(propertyName), value);
      
      byte v = message.getByteProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetBytePropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getByteProperty(propertyName);
         fail("can not get a null byte property");
      } catch (NumberFormatException e)
      {
      }
   }
   
   public void testGetBytePropertyWithString() throws Exception
   {
      byte b = randomByte();
      SimpleString value = new SimpleString(Byte.toString(b));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      byte v = message.getByteProperty(propertyName);
      assertEquals(b, v);
   }
   
   public void testGetBytePropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), randomFloat());

      try
      {
         message.getByteProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testSetShortProperty() throws Exception
   {
      short value = randomShort();

      JBossMessage message = new JBossMessage();
      message.setShortProperty(propertyName, value);

      short v = (Short) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testGetShortProperty() throws Exception
   {
      short value = randomShort();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putShortProperty(new SimpleString(propertyName), value);
      
      short v = message.getShortProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetShortPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getShortProperty(propertyName);
         fail("can not get a null byte property");
      } catch (NumberFormatException e)
      {
      }
   }
   
   public void testGetShortPropertyWithByte() throws Exception
   {
      byte value = randomByte();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putByteProperty(new SimpleString(propertyName), value);
      
      short v = message.getShortProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetShortPropertyWithString() throws Exception
   {
      short s = randomShort();
      SimpleString value = new SimpleString(Short.toString(s));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      short v = message.getShortProperty(propertyName);
      assertEquals(s, v);
   }
   
   public void testGetShortPropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), randomFloat());

      try
      {
         message.getShortProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testSetIntProperty() throws Exception
   {
      int value = randomInt();

      JBossMessage message = new JBossMessage();
      message.setIntProperty(propertyName, value);

      int v = (Integer) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testGetIntProperty() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putIntProperty(new SimpleString(propertyName), value);
      
      int v = message.getIntProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetIntPropertyWithJMSXDeliveryCount() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDeliveryCount(value);
      
      int v = message.getIntProperty(JBossMessage.JMSXDELIVERYCOUNT);
      assertEquals(value, v);
   }
   public void testGetIntPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getIntProperty(propertyName);
         fail("can not get a null byte property");
      } catch (NumberFormatException e)
      {
      }
   }
   
   public void testGetIntPropertyWithByte() throws Exception
   {
      byte value = randomByte();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putByteProperty(new SimpleString(propertyName), value);
      
      int v = message.getIntProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetIntPropertyWithShort() throws Exception
   {
      short value = randomShort();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putShortProperty(new SimpleString(propertyName), value);
      
      int v = message.getIntProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetIntPropertyWithString() throws Exception
   {
      int i = randomInt();
      SimpleString value = new SimpleString(Integer.toString(i));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      int v = message.getIntProperty(propertyName);
      assertEquals(i, v);
   }
   
   public void testGetIntPropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), randomFloat());

      try
      {
         message.getIntProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testSetLongProperty() throws Exception
   {
      long value = randomLong();

      JBossMessage message = new JBossMessage();
      message.setLongProperty(propertyName, value);

      long v = (Long) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testGetLongProperty() throws Exception
   {
      long value = randomLong();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putLongProperty(new SimpleString(propertyName), value);
      
      long v = message.getLongProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetLongPropertyWithJMSXDeliveryCount() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDeliveryCount(value);
      
      long v = message.getLongProperty(JBossMessage.JMSXDELIVERYCOUNT);
      assertEquals(value, v);
   }
   
   public void testGetLongPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getLongProperty(propertyName);
         fail("can not get a null byte property");
      } catch (NumberFormatException e)
      {
      }
   }
   
   public void testGetLongPropertyWithByte() throws Exception
   {
      byte value = randomByte();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putByteProperty(new SimpleString(propertyName), value);
      
      long v = message.getLongProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetLongPropertyWithShort() throws Exception
   {
      short value = randomShort();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putShortProperty(new SimpleString(propertyName), value);
      
      long v = message.getLongProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetLongPropertyWithInt() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putIntProperty(new SimpleString(propertyName), value);
      
      long v = message.getLongProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetLongPropertyWithString() throws Exception
   {
      long l = randomLong();
      SimpleString value = new SimpleString(Long.toString(l));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      long v = message.getLongProperty(propertyName);
      assertEquals(l, v);
   }
   
   public void testGetLongPropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), randomFloat());

      try
      {
         message.getLongProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testSetFloatProperty() throws Exception
   {
      float value = randomFloat();

      JBossMessage message = new JBossMessage();
      message.setFloatProperty(propertyName, value);

      float v = (Float) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }
   
   public void testGetFloatProperty() throws Exception
   {
      float value = randomFloat();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), value);
      
      float v = message.getFloatProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetFloatPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getFloatProperty(propertyName);
         fail("can not get a null float property");
      } catch (NullPointerException e)
      {
      }
   }
   
   public void testGetFloatPropertyWithString() throws Exception
   {
      float f = randomFloat();
      SimpleString value = new SimpleString(Float.toString(f));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      float v = message.getFloatProperty(propertyName);
      assertEquals(f, v);
   }
   
   public void testGetFloatPropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putBooleanProperty(new SimpleString(propertyName), true);

      try
      {
         message.getFloatProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testSetDoubleProperty() throws Exception
   {
      double value = randomDouble();

      JBossMessage message = new JBossMessage();
      message.setDoubleProperty(propertyName, value);

      double v = (Double) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }
   
   public void testGetDoubleProperty() throws Exception
   {
      double value = randomDouble();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putDoubleProperty(new SimpleString(propertyName), value);
      
      double v = message.getDoubleProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetDoublePropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      try
      {
         message.getDoubleProperty(propertyName);
         fail("can not get a null float property");
      } catch (NullPointerException e)
      {
      }
   }
   
   public void testGetDoublePropertyWithFloat() throws Exception
   {
      float value = randomFloat();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), value);
      
      double v = message.getDoubleProperty(propertyName);
      assertEquals(Float.valueOf(value).doubleValue(), v);
   }
   
   public void testGetDoublePropertyWithString() throws Exception
   {
      double d = randomDouble();
      SimpleString value = new SimpleString(Double.toString(d));
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), value);
      
      double v = message.getDoubleProperty(propertyName);
      assertEquals(d, v);
   }
   
   public void testGetDoublePropertyWithInvalidType() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putBooleanProperty(new SimpleString(propertyName), true);

      try
      {
         message.getDoubleProperty(propertyName);
         fail("invalid conversion");
      } catch (MessageFormatException e)
      {
      }
   }

   public void testSetStringProperty() throws Exception
   {
      String value = randomString();

      JBossMessage message = new JBossMessage();
      message.setStringProperty(propertyName, value);

      SimpleString v = (SimpleString) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v.toString());
   }

   public void testGetStringProperty() throws Exception
   {
      String value = randomString();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), new SimpleString(value));
      
      String v = message.getStringProperty(propertyName);
      assertEquals(value, v);
   }
   
   public void testGetStringPropertyWithJMSXDeliveryCount() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDeliveryCount(value);
      
      String v = message.getStringProperty(JBossMessage.JMSXDELIVERYCOUNT);
      assertEquals(Integer.toString(value), v);
   }
   
   public void testGetStringPropertyWithNull() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      String value = message.getStringProperty(propertyName);
      assertNull(value);
   }
   
   public void testGetStringPropertyWithBoolean() throws Exception
   {
      boolean value = true;
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putBooleanProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Boolean.toString(value), v);
   }
   
   public void testGetStringPropertyWithByte() throws Exception
   {
      byte value = randomByte();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putByteProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Byte.toString(value), v);
   }
   
   public void testGetStringPropertyWithShort() throws Exception
   {
      short value = randomShort();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putShortProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Short.toString(value), v);
   }
   
   public void testGetStringPropertyWithInt() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putIntProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Integer.toString(value), v);
   }
   
   public void testGetStringPropertyWithLong() throws Exception
   {
      long value = randomLong();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putLongProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Long.toString(value), v);
   }
   
   public void testGetStringPropertyWithFloat() throws Exception
   {
      float value = randomFloat();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putFloatProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Float.toString(value), v);
   }
   
   public void testGetStringPropertyWithDouble() throws Exception
   {
      double value = randomDouble();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putDoubleProperty(new SimpleString(propertyName), value);
      
      String v = message.getStringProperty(propertyName);
      assertEquals(Double.toString(value), v);
   }
   
   public void testSetObjectPropertyWithBoolean() throws Exception
   {
      boolean value = true;

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      boolean v = (Boolean) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithByte() throws Exception
   {
      byte value = randomByte();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      byte v = (Byte) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithShort() throws Exception
   {
      short value = randomShort();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      short v = (Short) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithInt() throws Exception
   {
      int value = randomInt();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      int v = (Integer) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithLong() throws Exception
   {
      long value = randomLong();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      long v = (Long) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithFloat() throws Exception
   {
      float value = randomFloat();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      float v = (Float) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithDouble() throws Exception
   {
      double value = randomDouble();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      double v = (Double) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v);
   }

   public void testSetObjectPropertyWithString() throws Exception
   {
      String value = randomString();

      JBossMessage message = new JBossMessage();
      message.setObjectProperty(propertyName, value);

      SimpleString v = (SimpleString) message.getCoreMessage().getProperty(
            new SimpleString(propertyName));
      assertEquals(value, v.toString());
   }

   public void testSetObjectPropertyWithInvalidType() throws Exception
   {

      JBossMessage message = new JBossMessage();
      try
      {
         message.setObjectProperty(propertyName, new Character('a'));
         fail("invalid type");
      } catch (MessageFormatException e)
      {
      }
   }
   
   public void testGetObjectProperty() throws Exception
   {
      double value = randomDouble();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putDoubleProperty(new SimpleString(propertyName), value);
      
      Object v = message.getObjectProperty(propertyName);
      assertTrue(v instanceof Double);
      assertEquals(value, v);
   }
   
   public void testGetObjectPropertyWithString() throws Exception
   {
      String value = randomString();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().putStringProperty(new SimpleString(propertyName), new SimpleString(value));
      
      Object v = message.getObjectProperty(propertyName);
      assertTrue(v instanceof String);
      assertEquals(value, v);
   }
   
   public void testGetObjectPropertyWithJMSXDeliveryCount() throws Exception
   {
      int value = randomInt();
      
      JBossMessage message = new JBossMessage();
      message.getCoreMessage().setDeliveryCount(value);
      
      Object v = message.getObjectProperty(JBossMessage.JMSXDELIVERYCOUNT);
      assertTrue(v instanceof String);
      assertEquals(Integer.toString(value), v);
   }

   public void testClearProperties() throws Exception
   {
      JBossMessage message = new JBossMessage();
      message.setBooleanProperty(propertyName, true);
      
      assertTrue(message.getBooleanProperty(propertyName));
      
      message.clearProperties();
      
      assertFalse(message.getBooleanProperty(propertyName));
   }
   
   public void testPropertyExists() throws Exception
   {
      JBossMessage message = new JBossMessage();
      
      assertFalse(message.propertyExists(propertyName));
      
      message.setBooleanProperty(propertyName, true);
      
      assertTrue(message.propertyExists(propertyName));
   }
   
   public void testAcknowledge() throws Exception
   {
      clientMessage = createStrictMock(ClientMessage.class);
      clientSession = createStrictMock(ClientSession.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      clientSession.commit();
      replay(clientMessage, clientSession, buffer);
      
      JBossMessage message = new JBossMessage(clientMessage, clientSession);      
      message.acknowledge();
      
      verify(clientMessage, clientSession, buffer);
   }
   
   public void testAcknowledgeThrowsException() throws Exception
   {
      clientMessage = createStrictMock(ClientMessage.class);
      clientSession = createStrictMock(ClientSession.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getBody()).andReturn(buffer);
      clientSession.commit();
      EasyMock.expectLastCall().andThrow(new MessagingException());
      replay(clientMessage, clientSession, buffer);
      
      JBossMessage message = new JBossMessage(clientMessage, clientSession);      
      try
      {
         message.acknowledge();
         fail("JMSException");
      } catch (JMSException e)
      {
      }
      
      verify(clientMessage, clientSession, buffer);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      propertyName = "property";
   }

   // Private -------------------------------------------------------

   private void doTestCreateMessage(byte expectedType, Class expectedInterface)
         throws Exception
   {
      clientSession = createStrictMock(ClientSession.class);
      clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      expect(clientMessage.getType()).andReturn(expectedType);
      expect(clientMessage.getBody()).andReturn(buffer);
      replay(clientSession, clientMessage, buffer);

      JBossMessage message = JBossMessage.createMessage(clientMessage,
            clientSession);
      assertEquals(expectedType, message.getType());
      assertTrue(message.getClass().isAssignableFrom(expectedInterface));

      verify(clientSession, clientMessage, buffer);
   }

   // Inner classes -------------------------------------------------
}
