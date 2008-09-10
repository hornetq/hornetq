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

package org.jboss.messaging.tests.unit.core.message.impl;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomFloat;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomShort;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.nio.ByteBuffer;
import java.util.Set;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class MessageImplTestBase extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(MessageImplTestBase.class);

   protected abstract Message createMessage(final byte type, final boolean durable, final long expiration,
                                   final long timestamp, final byte priority, MessagingBuffer buffer);
      
   protected abstract Message createMessage();
      
   public void testEncodeDecode()
   {
      for (int j = 0; j < 10; j++)
      {
         byte[] bytes = new byte[1000];
         for (int i = 0; i < bytes.length; i++)
         {
            bytes[i] = randomByte();
         }
         ByteBuffer bb = ByteBuffer.wrap(bytes);    
         MessagingBuffer body = new ByteBufferWrapper(bb);      
         Message message = createMessage(randomByte(), randomBoolean(), randomLong(),
                                         randomLong(), randomByte(), body);
         message.setDestination(new SimpleString("oasoas"));
         
         message.putStringProperty(new SimpleString("prop1"), new SimpleString("blah1"));
         message.putStringProperty(new SimpleString("prop2"), new SimpleString("blah2"));      
         ByteBuffer bbMsg = ByteBuffer.allocate(message.getEncodeSize());
         MessagingBuffer buffer = new ByteBufferWrapper(bbMsg);      
         message.encode(buffer);      
         Message message2 = createMessage();      
         buffer.flip();      
         message2.decode(buffer);      
         assertMessagesEquivalent(message, message2);
      }
   }
   
   public void getSetAttributes()
   {
      for (int j = 0; j < 10; j++)
      {
         byte[] bytes = new byte[1000];
         for (int i = 0; i < bytes.length; i++)
         {
            bytes[i] = randomByte();
         }
         ByteBuffer bb = ByteBuffer.wrap(bytes);    
         MessagingBuffer body = new ByteBufferWrapper(bb); 
         
         final byte type = randomByte();
         final boolean durable = randomBoolean();
         final long expiration = randomLong();
         final long timestamp = randomLong();
         final byte priority = randomByte();
   
         Message message = createMessage(type, durable, expiration,
                                         timestamp, priority, body);
         
         assertEquals(type, message.getType());
         assertEquals(durable, message.isDurable());
         assertEquals(expiration, message.getExpiration());
         assertEquals(timestamp, message.getTimestamp());
         assertEquals(priority, message.getPriority());
         
         final SimpleString destination = new SimpleString(randomString());
         final boolean durable2 = randomBoolean();
         final long expiration2 = randomLong();
         final long timestamp2 = randomLong();
         final byte priority2 = randomByte();
         
         message.setDestination(destination);
         assertEquals(destination, message.getDestination());
         
         message.setDurable(durable2);
         assertEquals(durable2, message.isDurable());
         
         message.setExpiration(expiration2);
         assertEquals(expiration2, message.getExpiration());
         
         message.setTimestamp(timestamp2);
         assertEquals(timestamp2, message.getTimestamp());
         
         message.setPriority(priority2);
         assertEquals(priority2, message.getPriority());
         
         message.setBody(body);
         assertTrue(body == message.getBody());
      }      
   }
   
   public void testExpired()
   {
      Message message = createMessage();
      
      assertEquals(0, message.getExpiration());
      assertFalse(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() + 1000);
      assertFalse(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() - 1);
      assertTrue(message.isExpired());
      
      message.setExpiration(System.currentTimeMillis() - 1000);
      assertTrue(message.isExpired());
      
      message.setExpiration(0);
      assertFalse(message.isExpired());
   }
   

   public void testEncodingMessage() throws Exception
   {
      byte[] bytes = new byte[]{(byte)1, (byte)2, (byte)3};
      final IoBufferWrapper bufferBody = new IoBufferWrapper(bytes.length);
      bufferBody.putBytes(bytes);
            
      SimpleString address = new SimpleString("Simple Destination ");
      
      Message msg = createMessage(); 
      msg.setBody(new ByteBufferWrapper(ByteBuffer.allocateDirect(1024)));
         
      msg.setDestination(address);
      msg.setBody(bufferBody);
      msg.putStringProperty(new SimpleString("Key"), new SimpleString("This String is worthless!"));
      msg.putStringProperty(new SimpleString("Key"), new SimpleString("This String is worthless and bigger!"));
      msg.putStringProperty(new SimpleString("Key2"), new SimpleString("This String is worthless and bigger and bigger!"));
      msg.removeProperty(new SimpleString("Key2"));

      checkSizes(msg, new ServerMessageImpl());

      msg.removeProperty(new SimpleString("Key"));
      
      checkSizes(msg, new ServerMessageImpl());
   }
   
   public void testProperties()
   {
      for (int j = 0; j < 10; j++)
      {
         Message msg = createMessage();
         
         SimpleString prop1 = new SimpleString("prop1");
         boolean val1 = randomBoolean();
         msg.putBooleanProperty(prop1, val1);
         
         SimpleString prop2 = new SimpleString("prop2");
         byte val2 = randomByte();
         msg.putByteProperty(prop2, val2);
         
         SimpleString prop3 = new SimpleString("prop3");
         byte[] val3 = randomBytes();
         msg.putBytesProperty(prop3, val3);
         
         SimpleString prop4 = new SimpleString("prop4");
         double val4 = randomDouble();
         msg.putDoubleProperty(prop4, val4);
         
         SimpleString prop5 = new SimpleString("prop5");
         float val5 = randomFloat();
         msg.putFloatProperty(prop5, val5);
         
         SimpleString prop6 = new SimpleString("prop6");
         int val6 = randomInt();
         msg.putIntProperty(prop6, val6);
         
         SimpleString prop7 = new SimpleString("prop7");
         long val7 = randomLong();
         msg.putLongProperty(prop7, val7);
         
         SimpleString prop8 = new SimpleString("prop8");
         short val8 = randomShort();
         msg.putShortProperty(prop8, val8);
         
         SimpleString prop9 = new SimpleString("prop9");
         SimpleString val9 = new SimpleString(randomString());
         msg.putStringProperty(prop9, val9);
         
         assertEquals(9, msg.getPropertyNames().size());
         assertTrue(msg.getPropertyNames().contains(prop1));
         assertTrue(msg.getPropertyNames().contains(prop2));
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         assertTrue(msg.containsProperty(prop1));
         assertTrue(msg.containsProperty(prop2));
         assertTrue(msg.containsProperty(prop3));
         assertTrue(msg.containsProperty(prop4));
         assertTrue(msg.containsProperty(prop5));
         assertTrue(msg.containsProperty(prop6));
         assertTrue(msg.containsProperty(prop7));
         assertTrue(msg.containsProperty(prop8));
         assertTrue(msg.containsProperty(prop9));
                 
         assertEquals(val1, msg.getProperty(prop1));
         assertEquals(val2, msg.getProperty(prop2));
         assertEquals(val3, msg.getProperty(prop3));
         assertEquals(val4, msg.getProperty(prop4));
         assertEquals(val5, msg.getProperty(prop5));
         assertEquals(val6, msg.getProperty(prop6));
         assertEquals(val7, msg.getProperty(prop7));
         assertEquals(val8, msg.getProperty(prop8));
         assertEquals(val9, msg.getProperty(prop9));
         
         SimpleString val10 = new SimpleString(randomString());
         //test overwrite
         msg.putStringProperty(prop9, val10);
         assertEquals(val10, msg.getProperty(prop9));
         
         int val11 = randomInt();
         msg.putIntProperty(prop9, val11);
         assertEquals(val11, msg.getProperty(prop9));
         
         msg.removeProperty(prop1);
         assertEquals(8, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop2));
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         msg.removeProperty(prop2);
         assertEquals(7, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
         assertTrue(msg.getPropertyNames().contains(prop9));
         
         msg.removeProperty(prop9);
         assertEquals(6, msg.getPropertyNames().size());        
         assertTrue(msg.getPropertyNames().contains(prop3));
         assertTrue(msg.getPropertyNames().contains(prop4));
         assertTrue(msg.getPropertyNames().contains(prop5));
         assertTrue(msg.getPropertyNames().contains(prop6));
         assertTrue(msg.getPropertyNames().contains(prop7));
         assertTrue(msg.getPropertyNames().contains(prop8));
        
         msg.removeProperty(prop3);
         msg.removeProperty(prop4);
         msg.removeProperty(prop5);
         msg.removeProperty(prop6);
         msg.removeProperty(prop7);
         msg.removeProperty(prop8);   
         assertEquals(0, msg.getPropertyNames().size());               
      }            
   }
   
   // Protected -------------------------------------------------------------------------------
   
   protected void assertMessagesEquivalent(final Message msg1, final Message msg2)
   {
      assertEquals(msg1.isDurable(), msg2.isDurable());

      assertEquals(msg1.getExpiration(), msg2.getExpiration());

      assertEquals(msg1.getTimestamp(), msg2.getTimestamp());

      assertEquals(msg1.getPriority(), msg2.getPriority());

      assertEquals(msg1.getType(), msg2.getType());         

      assertEqualsByteArrays(msg1.getBody().array(), msg2.getBody().array());      

      assertEquals(msg1.getDestination(), msg2.getDestination());
      
      Set<SimpleString> props1 = msg1.getPropertyNames();
      
      Set<SimpleString> props2 = msg2.getPropertyNames();
      
      assertEquals(props1.size(), props2.size());
      
      for (SimpleString propname: props1)
      {
         Object val1 = msg1.getProperty(propname);
         
         Object val2 = msg2.getProperty(propname);
         
         assertEquals(val1, val2);
      }
   }
   
   // Private ----------------------------------------------------------------------------------
   
   private void checkSizes(final Message obj, final EncodingSupport newObject)
   {
      ByteBuffer bf = ByteBuffer.allocateDirect(1024);
      ByteBufferWrapper buffer = new ByteBufferWrapper(bf);
      obj.encode(buffer);
      assertEquals (buffer.position(), obj.getEncodeSize());
      int originalSize = buffer.position();
      
      bf.rewind();
      newObject.decode(buffer);
      
      bf = ByteBuffer.allocateDirect(1024 * 10);
      buffer = new ByteBufferWrapper(bf);
      
      newObject.encode(buffer);
      
      assertEquals(newObject.getEncodeSize(), bf.position());
      assertEquals(originalSize, bf.position());     
   }  
}
