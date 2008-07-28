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
package org.jboss.messaging.tests.unit.core.client.impl;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.tests.unit.core.message.impl.MessageImplTestBase;
import org.jboss.messaging.tests.util.RandomUtil;


/**
 * 
 * A ClientMessageImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientMessageImplTest extends MessageImplTestBase
{
   protected Message createMessage(final byte type, final boolean durable, final long expiration,
         final long timestamp, final byte priority, MessagingBuffer buffer)
   {      
      return new ClientMessageImpl(type, durable, expiration, timestamp, priority, buffer);
   }

   protected Message createMessage()
   {      
      return new ClientMessageImpl();
   }
   
   public void testDeliveryCountDeliveryIDConstructor()
   {
      final int deliveryCount = 120982;
      final long deliveryID = 10291029;
      ClientMessage msg = new ClientMessageImpl(deliveryCount, deliveryID);
      assertEquals(deliveryCount, msg.getDeliveryCount());
      assertEquals(deliveryID, msg.getDeliveryID());
   }
   
   public void testSetDeliveryCount()
   {
      ClientMessage msg = new ClientMessageImpl();
      final int deliveryCount = 127627;
      msg.setDeliveryCount(deliveryCount);
      assertEquals(deliveryCount, msg.getDeliveryCount());
   }
   
   public void testSetDeliveryID()
   {
      ClientMessage msg = new ClientMessageImpl();
      final long deliveryID = 1029123843;
      msg.setDeliveryID(deliveryID);
      assertEquals(deliveryID, msg.getDeliveryID());
   }
   
   public void testConstructor1()
   {
      for (int i = 0; i < 10; i++)
      {
         boolean durable = RandomUtil.randomBoolean();    
         ByteBuffer bb = ByteBuffer.wrap(new byte[1000]);    
         MessagingBuffer body = new ByteBufferWrapper(bb);   
         
         ClientMessage msg = new ClientMessageImpl(durable, body);
         assertEquals(durable, msg.isDurable());
         assertEquals(body, msg.getBody());
             
         assertEquals(0, msg.getType());
         assertEquals(0, msg.getExpiration());
         assertTrue(System.currentTimeMillis() - msg.getTimestamp() < 5);
         assertEquals((byte)4, msg.getPriority());
      }
   }
   
   public void testConstructor2()
   {
      for (int i = 0; i < 10; i++)
      {
         byte type = RandomUtil.randomByte();
         boolean durable = RandomUtil.randomBoolean();    
         ByteBuffer bb = ByteBuffer.wrap(new byte[1000]);    
         MessagingBuffer body = new ByteBufferWrapper(bb);   
         
         ClientMessage msg = new ClientMessageImpl(type, durable, body);
         assertEquals(type, msg.getType());
         assertEquals(durable, msg.isDurable());
         assertEquals(body, msg.getBody());
             
         assertEquals(0, msg.getExpiration());
         assertTrue(System.currentTimeMillis() - msg.getTimestamp() < 5);
         assertEquals((byte)4, msg.getPriority());
      }
   }
   
   public void testConstructor3()
   {
      for (int i = 0; i < 10; i++)
      {
         byte type = RandomUtil.randomByte();
         boolean durable = RandomUtil.randomBoolean();    
         ByteBuffer bb = ByteBuffer.wrap(new byte[1000]);    
         MessagingBuffer body = new ByteBufferWrapper(bb);   
         long expiration = RandomUtil.randomLong();
         long timestamp = RandomUtil.randomLong();
         byte priority = RandomUtil.randomByte();
         
         ClientMessage msg = new ClientMessageImpl(type, durable, expiration, timestamp, priority, body);
         assertEquals(type, msg.getType());
         assertEquals(durable, msg.isDurable());
         assertEquals(expiration, msg.getExpiration());
         assertEquals(timestamp, msg.getTimestamp());
         assertEquals(priority, msg.getPriority());
         assertEquals(body, msg.getBody());                      
      }
   }
}
