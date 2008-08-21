/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.expect;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomChar;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomShort;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import static org.jboss.messaging.tests.util.UnitTestCase.assertEqualsByteArrays;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.client.JBossBytesMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossBytesMessageTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testForeignBytesMessage() throws Exception
   {
      ClientSession session = EasyMock.createNiceMock(ClientSession.class);
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocate(3000));
      ClientMessage clientMessage = new ClientMessageImpl(JBossBytesMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      ByteBufferWrapper body2 = new ByteBufferWrapper(ByteBuffer.allocate(3000));
      ClientMessage clientMessage2 = new ClientMessageImpl(JBossBytesMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body2);
      expect(session.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);
      expect(session.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage2);
      EasyMock.replay(session);
      byte[] foreignBytes = randomBytes(3000);
      JBossBytesMessage foreignMessage = new JBossBytesMessage(session);
      foreignMessage.writeBytes(foreignBytes);
      foreignMessage.reset();
      JBossBytesMessage message = new JBossBytesMessage(foreignMessage, session);
      byte[] b = new byte[(int) foreignMessage.getBodyLength()];
      message.reset();

      message.readBytes(b);

      assertEqualsByteArrays(foreignBytes, b);
      EasyMock.verify(session);
   }
   
   public void testGetType() throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();
      assertEquals(JBossBytesMessage.TYPE, message.getType());
   }

   public void testGetBodyLength() throws Exception
   {
      byte[] value = randomBytes(1023);

      JBossBytesMessage message = new JBossBytesMessage();
      message.writeBytes(value);
      message.reset();

      assertEquals(1023, message.getBodyLength());

   }
   
   public void testClearBody() throws Exception
   {
      byte[] value = randomBytes();

      JBossBytesMessage message = new JBossBytesMessage();
      message.writeBytes(value);
      message.reset();

      assertTrue(message.getBodyLength() > 0);
      
      message.clearBody();
      message.reset();
      assertEquals(0, message.getBodyLength());
   }


   public void testWriteBoolean() throws Exception
   {
      boolean value = randomBoolean();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeBoolean(value);

      message.reset();

      assertEquals(value, message.readBoolean());
   }

   public void testReadBoolean() throws Exception
   {
      boolean value = randomBoolean();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putBoolean(value);
      message.reset();

      assertEquals(value, message.readBoolean());
   }

   public void testReadBooleanFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testWriteByte() throws Exception
   {
      byte value = randomByte();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeByte(value);

      message.reset();

      assertEquals(value, message.readByte());
   }

   public void testReadByte() throws Exception
   {
      byte value = randomByte();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putByte(value);
      message.reset();

      assertEquals(value, message.readByte());
   }

   public void testReadByteFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testWriteBytes() throws Exception
   {
      byte[] value = randomBytes();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeBytes(value);

      message.reset();

      byte[] v = new byte[value.length];
      message.readBytes(v);
      assertEqualsByteArrays(value, v);
   }

   public void testReadBytes() throws Exception
   {
      byte[] value = randomBytes();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putBytes(value);
      message.reset();

      byte[] v = new byte[value.length];
      message.readBytes(v);
      assertEqualsByteArrays(value, v);
   }

   public void testReadBytesFromEmptyMessage() throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();
      message.reset();

      byte[] v = new byte[1];
      int read = message.readBytes(v);
      assertEquals(-1, read);
   }

   public void testWriteShort() throws Exception
   {
      short value = randomShort();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeShort(value);

      message.reset();

      assertEquals(value, message.readShort());
   }

   public void testReadShort() throws Exception
   {
      short value = randomShort();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putShort(value);
      message.reset();

      assertEquals(value, message.readShort());
   }

   public void testReadShortFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testWriteChar() throws Exception
   {
      char value = randomChar();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeChar(value);

      message.reset();

      assertEquals(value, message.readChar());
   }

   public void testReadChar() throws Exception
   {
      char value = randomChar();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putChar(value);
      message.reset();

      assertEquals(value, message.readChar());
   }

   public void testReadCharFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testWriteInt() throws Exception
   {
      int value = randomInt();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeInt(value);

      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadInt() throws Exception
   {
      int value = randomInt();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putInt(value);
      message.reset();

      assertEquals(value, message.readInt());
   }

   public void testReadIntFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testWriteLong() throws Exception
   {
      long value = randomLong();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeLong(value);

      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLong() throws Exception
   {
      long value = randomLong();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putLong(value);
      message.reset();

      assertEquals(value, message.readLong());
   }

   public void testReadLongFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testWriteFloat() throws Exception
   {
      float value = randomFloat();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeFloat(value);

      message.reset();

      assertEquals(value, message.readFloat());
   }

   public void testReadFloat() throws Exception
   {
      float value = randomFloat();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putFloat(value);
      message.reset();

      assertEquals(value, message.readFloat());
   }

   public void testReadFloatFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testWriteDouble() throws Exception
   {
      double value = randomDouble();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeDouble(value);

      message.reset();

      assertEquals(value, message.readDouble());
   }

   public void testReadDouble() throws Exception
   {
      double value = randomDouble();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putDouble(value);
      message.reset();

      assertEquals(value, message.readDouble());
   }

   public void testReadDoubleFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testWriteUTF() throws Exception
   {
      String value = randomString();
      JBossBytesMessage message = new JBossBytesMessage();
      message.writeUTF(value);

      message.reset();

      assertEquals(value, message.readUTF());
   }

   public void testReadUTF() throws Exception
   {
      String value = randomString();
      JBossBytesMessage message = new JBossBytesMessage();

      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putUTF(value);
      message.reset();

      assertEquals(value, message.readUTF());
   }

   public void testReadUTFFromEmptyMessage() throws Exception
   {
      doReadFromEmptyMessage(new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readUTF();
         }
      });
   }

   public void testWriteObjectWithNull() throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();

      try
      {
         message.writeObject(null);
         fail("NullPointerException");
      } catch (NullPointerException e)
      {
      }
   }

   public void testWriteObjectWithString() throws Exception
   {
      doWriteObjectWithType(randomString(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readUTF();
         }
      });
   }

   public void testWriteObjectWithBoolean() throws Exception
   {
      doWriteObjectWithType(randomBoolean(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readBoolean();
         }
      });
   }

   public void testWriteObjectWithByte() throws Exception
   {
      doWriteObjectWithType(randomByte(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readByte();
         }
      });
   }

   public void testWriteObjectWithBytes() throws Exception
   {
      doWriteObjectWithType(randomBytes(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            byte[] bytes = new byte[(int) message.getBodyLength()];
            message.readBytes(bytes);
            return bytes;
         }
      });
   }

   public void testWriteObjectWithShort() throws Exception
   {
      doWriteObjectWithType(randomShort(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readShort();
         }
      });
   }

   public void testWriteObjectWithChar() throws Exception
   {
      doWriteObjectWithType(randomChar(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readChar();
         }
      });
   }

   public void testWriteObjectWithInt() throws Exception
   {
      doWriteObjectWithType(randomInt(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readInt();
         }
      });
   }

   public void testWriteObjectWithLong() throws Exception
   {
      doWriteObjectWithType(randomLong(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readLong();
         }
      });
   }

   public void testWriteObjectWithFloat() throws Exception
   {
      doWriteObjectWithType(randomFloat(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readFloat();
         }
      });
   }

   public void testWriteObjectWithDouble() throws Exception
   {
      doWriteObjectWithType(randomDouble(), new TypeReader()
      {
         public Object readType(JBossBytesMessage message) throws Exception
         {
            return message.readDouble();
         }
      });
   }

   public void testWriteObjectWithInvalidType() throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();

      try
      {
         message.writeObject(new ArrayList());
         fail("MessageFormatException");
      } catch (MessageFormatException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doReadFromEmptyMessage(TypeReader reader) throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();
      message.reset();

      try
      {
         reader.readType(message);
         fail("must throw a MessageEOFException");
      } catch (MessageEOFException e)
      {
      }
   }

   private void doWriteObjectWithType(Object value, TypeReader reader)
         throws Exception
   {
      JBossBytesMessage message = new JBossBytesMessage();

      message.writeObject(value);
      message.reset();

      Object v = reader.readType(message);
      if (v instanceof byte[])
      {
         assertEqualsByteArrays((byte[]) value, (byte[]) v);
      } else
      {
         assertEquals(value, v);
      }
   }

   // Inner classes -------------------------------------------------

   private interface TypeReader
   {
      Object readType(JBossBytesMessage message) throws Exception;
   }
}
