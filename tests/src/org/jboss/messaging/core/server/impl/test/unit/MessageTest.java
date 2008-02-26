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
package org.jboss.messaging.core.server.impl.test.unit;

import static org.jboss.messaging.test.unit.RandomUtil.randomByte;
import static org.jboss.messaging.test.unit.RandomUtil.randomInt;
import static org.jboss.messaging.test.unit.RandomUtil.randomLong;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.server.Message;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.impl.MessageImpl;
import org.jboss.messaging.core.server.impl.test.unit.fakes.FakeQueueFactory;
import org.jboss.messaging.test.unit.UnitTestCase;
import org.jboss.messaging.util.StreamUtils;

/**
 * 
 * Tests for Message and MessageReference
 * 
 * TODO - Test streaming and destreaming
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessageTest extends UnitTestCase
{
	private QueueFactory queueFactory = new FakeQueueFactory();
   
   public void testCreateMessage()
   {
      long id = 56465;
      int type = 655;
      boolean reliable = true;
      long expiration = 6712671;
      long timestamp = 82798172;
      byte priority = 32;
      
      Message message = new MessageImpl(type, reliable, expiration, timestamp, priority);
      message.setMessageID(id);
      
      assertEquals(id, message.getMessageID());
      assertEquals(type, message.getType());
      assertEquals(reliable, message.isDurable());
      assertEquals(timestamp, message.getTimestamp());
      assertEquals(priority, message.getPriority());
      
      reliable = false;
      
      message = new MessageImpl(type, reliable, expiration, timestamp, priority);
      message.setMessageID(id);
      
      assertEquals(id, message.getMessageID());
      assertEquals(type, message.getType());
      assertEquals(reliable, message.isDurable());
      assertEquals(timestamp, message.getTimestamp());
      assertEquals(priority, message.getPriority());
   }
   
   public void testCreateMessageFromStorage() throws Exception
   {
      long id = 56465;
      int type = 655;
      boolean reliable = true;
      long expiration = 6712671;
      long timestamp = 82798172;
      byte priority = 32;
      
      byte[] bytes = "blah blah blah".getBytes();
 
      Message message = new MessageImpl(id, type, reliable, expiration, timestamp, priority,
            null, bytes);
      
      assertEquals(id, message.getMessageID());
      assertEquals(type, message.getType());
      assertEquals(reliable, message.isDurable());
      assertEquals(timestamp, message.getTimestamp());
      assertEquals(priority, message.getPriority());     
      
      assertByteArraysEquivalent(bytes, message.getPayload());   
      
      //TODO - headers - they should really be combined into single blob
   }
   
   public void testCopy()
   {
      long id = 56465;
      int type = 655;
      boolean reliable = true;
      long expiration = 6712671;
      long timestamp = 82798172;
      byte priority = 32;
      
      Message message = new MessageImpl(type, reliable, expiration, timestamp, priority);
      message.setMessageID(id);
      
      Message message2 = message.copy();
      
      assertEquivalent(message, message2);
   }
   
   public void testSetAndGetMessageID()
   {
      Message message = new MessageImpl();
      
      assertEquals(0, message.getMessageID());
      
      message = new MessageImpl(655, true, 767676, 989898, (byte)32);
      
      assertEquals(0, message.getMessageID());
      
      long id = 765432;
      message.setMessageID(id);
      assertEquals(id, message.getMessageID());
   }
   
   public void testSetAndGetReliable()
   {
      Message message = new MessageImpl();
      
      boolean reliable = true;
      message.setDurable(reliable);
      assertEquals(reliable, message.isDurable());
      
      reliable = false;
      message.setDurable(reliable);
      assertEquals(reliable, message.isDurable());
   }
    
   public void testSetAndGetExpiration()
   {
      Message message = new MessageImpl();
      
      long expiration = System.currentTimeMillis() + 10000;
      message.setExpiration(expiration);
      assertEquals(expiration, message.getExpiration());
      assertFalse(message.isExpired());
      message.setExpiration(System.currentTimeMillis() - 1);
      assertTrue(message.isExpired());
      
      expiration = 0; //O means never expire
      message.setExpiration(expiration);
      assertEquals(expiration, message.getExpiration());
      assertFalse(message.isExpired());
   }
      
   public void testSetAndGetTimestamp()
   {
      Message message = new MessageImpl();
      
      long timestamp = System.currentTimeMillis();
      message.setTimestamp(timestamp);
      assertEquals(timestamp, message.getTimestamp());
   }
   
   public void testSetAndGetPriority()
   {
      Message message = new MessageImpl();
      byte priority = 7;
      message.setPriority(priority);
      assertEquals(priority, message.getPriority());
   }
   
   public void testSetAndGetConnectionID()
   {
      Message message = new MessageImpl();
      
      assertNull(message.getConnectionID());
      String connectionID = "conn123";
      message.setConnectionID(connectionID);
      assertEquals(connectionID, message.getConnectionID());      
   }
   
   public void testSetAndGetPayload()
   {
      Message message = new MessageImpl();
      
      assertNull(message.getPayload());
      
      byte[] bytes = "blah blah blah".getBytes();
      message.setPayload(bytes);
      
      assertByteArraysEquivalent(bytes, message.getPayload());            
   }
   
   public void testHeaders()
   {
      Message message = new MessageImpl();
      
      assertNotNull(message.getHeaders());
      assertTrue(message.getHeaders().isEmpty());
      
      String key1 = "key1";
      String val1 = "wibble";
      String key2 = "key2";
      Object val2 = new Object();
      String key3 = "key3";
      Double val3 = new Double(123.456);
      Long val4 = new Long(77777);
      message.putHeader(key1, val1);
      assertEquals(val1, message.getHeaders().get(key1));
      assertEquals(1, message.getHeaders().size());
      assertTrue(message.containsHeader(key1));
      assertFalse(message.containsHeader("does not exist"));
      message.putHeader(key2, val2);
      assertEquals(val2, message.getHeaders().get(key2));
      assertEquals(2, message.getHeaders().size());
      assertTrue(message.containsHeader(key2));
      message.putHeader(key3, val3);
      assertEquals(val3, message.getHeaders().get(key3));
      assertEquals(3, message.getHeaders().size());
      assertTrue(message.containsHeader(key3));
      message.putHeader(key3, val4);
      assertEquals(val4, message.getHeaders().get(key3));
      assertEquals(3, message.getHeaders().size());
      assertEquals(val2, message.removeHeader(key2));
      assertEquals(2, message.getHeaders().size());
      assertFalse(message.containsHeader(key2));
      assertNull(message.removeHeader("does not exist"));
      assertEquals(val1, message.removeHeader(key1));
      assertFalse(message.containsHeader(key2));
      assertEquals(1, message.getHeaders().size());
      assertEquals(val4, message.removeHeader(key3));
      assertFalse(message.containsHeader(key3));
      assertTrue(message.getHeaders().isEmpty());
   }
   
   public void testEquals()
   {
      Message message1 = new MessageImpl();
      message1.setMessageID(1);
      
      Message message2 = new MessageImpl();
      message2.setMessageID(2);
      
      Message message3 = new MessageImpl();
      message3.setMessageID(1);
      
      assertTrue(message1.equals(message1));
      assertTrue(message2.equals(message2));
      assertTrue(message3.equals(message3));
      
      assertFalse(message1.equals(message2));
      assertFalse(message2.equals(message1));
      
      assertFalse(message2.equals(message3));
      assertFalse(message3.equals(message2));
      
      assertTrue(message1.equals(message3));
      assertTrue(message3.equals(message1));
      
   }
   
   public void testHashcode()
   {
      long id1 = 6567575;
      Message message1 = new MessageImpl();
      message1.setMessageID(id1);
      
      assertEquals((int) ((id1 >>> 32) ^ id1), message1.hashCode());
   }
   
   public void testMessageReference()
   {
      Message message = new MessageImpl();
      
      assertTrue(message.getReferences().isEmpty());
      
      Queue queue1 = queueFactory.createQueue(1, "queue1", null, false, true);
      Queue queue2 = queueFactory.createQueue(2, "queue2", null, false, true);

      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      MessageReference ref1 = message.createReference(queue1);
      refs.add(ref1);
      MessageReference ref2 = message.createReference(queue2);
      refs.add(ref2);
      MessageReference ref3 = message.createReference(queue1);
      refs.add(ref3);
      MessageReference ref4 = message.createReference(queue2);
      refs.add(ref4);
      
      assertRefListsIdenticalRefs(refs, message.getReferences());
      
      assertEquals(queue1, ref1.getQueue());
      assertEquals(queue2, ref2.getQueue());
      assertEquals(queue1, ref3.getQueue());
      assertEquals(queue2, ref4.getQueue());
      
      int deliveryCount = 65235;
      ref1.setDeliveryCount(deliveryCount);
      assertEquals(deliveryCount, ref1.getDeliveryCount());
      
      long scheduledDeliveryTime = 908298123;
      ref1.setScheduledDeliveryTime(scheduledDeliveryTime);
      assertEquals(scheduledDeliveryTime, ref1.getScheduledDeliveryTime());
      
      Queue queue3 = queueFactory.createQueue(3, "queue3", null, false, true);
      MessageReference ref5 = ref1.copy(queue3);
      
      assertEquals(deliveryCount, ref5.getDeliveryCount());
      assertEquals(scheduledDeliveryTime, ref5.getScheduledDeliveryTime());
      assertEquals(queue3, ref5.getQueue());
   }
   

   public void testDurableReferences()
   {
      Message messageDurable = new MessageImpl();
      messageDurable.setDurable(true);
      
      Message messageNonDurable = new MessageImpl();
      messageNonDurable.setDurable(false);
        
      //Durable queue
      Queue queue1 = queueFactory.createQueue(1, "queue1", null, true, false);
      
      //Non durable queue
      Queue queue2 = queueFactory.createQueue(2, "queue2", null, false, false);
      
      assertEquals(0, messageDurable.getNumDurableReferences());
      
      MessageReference ref1 = messageDurable.createReference(queue1);
      
      assertEquals(1, messageDurable.getNumDurableReferences());
      
      MessageReference ref2 = messageDurable.createReference(queue2);
      
      assertEquals(1, messageDurable.getNumDurableReferences());
      
      assertEquals(0, messageNonDurable.getNumDurableReferences());
      
      MessageReference ref3 = messageNonDurable.createReference(queue1);
      
      assertEquals(0, messageNonDurable.getNumDurableReferences());
      
      MessageReference ref4 = messageNonDurable.createReference(queue2);
      
      assertEquals(0, messageNonDurable.getNumDurableReferences());
                  
   }
   
   public void testDurableReferencePos()
   {
      Message messageDurable = new MessageImpl();
      messageDurable.setDurable(true);
      
      //Durable queue
      Queue queue1 = queueFactory.createQueue(1, "queue1", null, true, false);
      
      //Non durable queue
      Queue queue2 = queueFactory.createQueue(2, "queue2", null, false, false);
      
      
      MessageReference ref1 = messageDurable.createReference(queue1);
      
      MessageReference ref2 = messageDurable.createReference(queue2);
      
      MessageReference ref3 = messageDurable.createReference(queue2);
      
      MessageReference ref4 = messageDurable.createReference(queue1);
      
      MessageReference ref5 = messageDurable.createReference(queue1);
      
      MessageReference ref6 = messageDurable.createReference(queue2);
      
      MessageReference ref7 = messageDurable.createReference(queue1);
      
      MessageReference ref8 = messageDurable.createReference(queue2);
      
      assertEquals(0, messageDurable.getDurableReferencePos(ref1));
      
      assertEquals(1, messageDurable.getDurableReferencePos(ref4));
      
      assertEquals(2, messageDurable.getDurableReferencePos(ref5));
      
      assertEquals(3, messageDurable.getDurableReferencePos(ref7));
             
   }
   
   public void testMarshalling() throws Exception
   {
      Message msg = new MessageImpl(randomLong(), randomInt(), true, randomLong(), randomLong(), randomByte(),null, null);
      msg.setDeliveryCount(randomInt());
      
      byte[] bytes = StreamUtils.toBytes(msg);
      Message unmarshalledMsg = new MessageImpl();
      StreamUtils.fromBytes(unmarshalledMsg, bytes);
      
      assertEquals(msg, unmarshalledMsg);
      assertEquals("messageID", msg.getMessageID(), unmarshalledMsg.getMessageID());
      assertEquals("type", msg.getType(), unmarshalledMsg.getType());
      assertEquals("durable", msg.isDurable(), unmarshalledMsg.isDurable());
      assertEquals("expiration", msg.getExpiration(), unmarshalledMsg.getExpiration());
      assertEquals("timestamp", msg.getTimestamp(), unmarshalledMsg.getTimestamp());
      assertEquals("priority", msg.getPriority(), unmarshalledMsg.getPriority());
      assertEquals("deliveryCount", msg.getDeliveryCount(), unmarshalledMsg.getDeliveryCount()); 
   }
   
}
