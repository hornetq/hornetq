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
package org.jboss.messaging.tests.unit.core.message.impl;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

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
   
   public void testCreateMessageBeforeSending()
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

      Message message = new MessageImpl(id);
      
      assertEquals(id, message.getMessageID());
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
      
      assertEquals(0, message.getConnectionID());
      long connectionID = 781628;
      message.setConnectionID(connectionID);
      assertEquals(connectionID, message.getConnectionID());      
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
      
      SimpleString squeue1 = new SimpleString("queue1");
      SimpleString squeue2 = new SimpleString("queue2");
      SimpleString squeue3 = new SimpleString("queue3");
      
      Queue queue1 = queueFactory.createQueue(1, squeue1, null, false, true);
      Queue queue2 = queueFactory.createQueue(2, squeue2, null, false, true);
   
      MessageReference ref1 = message.createReference(queue1);
      MessageReference ref2 = message.createReference(queue2);
      MessageReference ref3 = message.createReference(queue1);
      MessageReference ref4 = message.createReference(queue2);
      
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
      
      Queue queue3 = queueFactory.createQueue(3, squeue3, null, false, true);
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
      
      SimpleString squeue1 = new SimpleString("queue1");
      SimpleString squeue2 = new SimpleString("queue2");
        
      //Durable queue
      Queue queue1 = queueFactory.createQueue(1, squeue1, null, true, false);
      
      //Non durable queue
      Queue queue2 = queueFactory.createQueue(2, squeue2, null, false, false);
      
      assertEquals(0, messageDurable.getDurableRefCount());
      
      MessageReference ref1 = messageDurable.createReference(queue1);
      
      assertEquals(1, messageDurable.getDurableRefCount());
      
      MessageReference ref2 = messageDurable.createReference(queue2);
      
      assertEquals(1, messageDurable.getDurableRefCount());
      
      assertEquals(0, messageNonDurable.getDurableRefCount());
      
      MessageReference ref3 = messageNonDurable.createReference(queue1);
      
      assertEquals(0, messageNonDurable.getDurableRefCount());
      
      MessageReference ref4 = messageNonDurable.createReference(queue2);
      
      assertEquals(0, messageNonDurable.getDurableRefCount());
               
      MessageReference ref5 = messageDurable.createReference(queue1);
      
      assertEquals(2, messageDurable.getDurableRefCount());
      
      messageDurable.decrementDurableRefCount();
      
      assertEquals(1, messageDurable.getDurableRefCount());
      
      messageDurable.decrementDurableRefCount();
      
      assertEquals(0, messageDurable.getDurableRefCount());
      
      messageDurable.incrementDurableRefCount();
      
      assertEquals(1, messageDurable.getDurableRefCount());                 
   }
   

   
}
