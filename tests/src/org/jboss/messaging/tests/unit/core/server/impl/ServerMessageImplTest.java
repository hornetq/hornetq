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
package org.jboss.messaging.tests.unit.core.server.impl;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.nio.ByteBuffer;

import org.easymock.EasyMock;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.tests.unit.core.message.impl.MessageImplTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ServerMessageImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerMessageImplTest extends MessageImplTestBase
{
   protected Message createMessage(final byte type, final boolean durable, final long expiration,
         final long timestamp, final byte priority, MessagingBuffer buffer)
   {      
      return new ServerMessageImpl(type, durable, expiration, timestamp, priority, buffer);
   }

   protected Message createMessage()
   {      
      return new ServerMessageImpl();
   }
   
   public void testMessageIDConstructor()
   {
      final long id = 1029812;
      ServerMessage msg = new ServerMessageImpl(id);
      assertEquals(id, msg.getMessageID());
   }
   
   public void testCopyConstructor()
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
         ServerMessageImpl message = new ServerMessageImpl(randomByte(), randomBoolean(), randomLong(),
                                         randomLong(), randomByte(), body);
         message.setDestination(new SimpleString("oasoas"));
         
         message.putStringProperty(new SimpleString("prop1"), new SimpleString("blah1"));
         message.putStringProperty(new SimpleString("prop2"), new SimpleString("blah2"));      
         
         message.setMessageID(81721872);
         message.setConnectionID(10291092);
         
         ServerMessage message2 = new ServerMessageImpl(message);
         
         assertMessagesEquivalent(message, message2);
         
         ServerMessage message3 = message2.copy();
         
         assertMessagesEquivalent(message2, message3);
      }
   }
   
   public void testGetSetMessageID()
   {
      ServerMessage msg = (ServerMessage)createMessage();      
      final long id = 91829182;
      msg.setMessageID(id);
      assertEquals(id, msg.getMessageID());
   }
   
   public void testGetSetConnectionID()
   {
      ServerMessage msg = (ServerMessage)createMessage();      
      final long id = 8172718;
      msg.setConnectionID(id);
      assertEquals(id, msg.getConnectionID());
   }
   
   public void testCreateReferencesDurable()
   {
      ServerMessage msg = new ServerMessageImpl();
      msg.setDurable(true);
      
      Queue queue1 = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue1.isDurable()).andReturn(true);
      EasyMock.expect(queue2.isDurable()).andReturn(true);
      EasyMock.expect(queue3.isDurable()).andReturn(false);
      
      EasyMock.replay(queue1, queue2, queue3);
      
      MessageReference ref1 = msg.createReference(queue1);
      MessageReference ref2 = msg.createReference(queue2);
      MessageReference ref3 = msg.createReference(queue3);
      
      assertEquals(msg, ref1.getMessage());
      assertEquals(msg, ref2.getMessage());
      assertEquals(msg, ref3.getMessage());
      
      assertEquals(queue1, ref1.getQueue());
      assertEquals(queue2, ref2.getQueue());
      assertEquals(queue3, ref3.getQueue());
      
      EasyMock.verify(queue1, queue2, queue3);
      
      assertEquals(2, msg.getDurableRefCount());
      
      msg.incrementDurableRefCount();
      assertEquals(3, msg.getDurableRefCount());
      
      msg.incrementDurableRefCount();
      assertEquals(4, msg.getDurableRefCount());
      
      msg.decrementDurableRefCount();
      assertEquals(3, msg.getDurableRefCount());
      
      msg.decrementDurableRefCount();
      assertEquals(2, msg.getDurableRefCount());
      
      msg.decrementDurableRefCount();      
      msg.decrementDurableRefCount();
      assertEquals(0, msg.getDurableRefCount());
   }
   
   public void testCreateReferencesNonDurable()
   {
      ServerMessage msg = new ServerMessageImpl();
      msg.setDurable(false);
      
      Queue queue1 = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.replay(queue1, queue2, queue3);
      
      MessageReference ref1 = msg.createReference(queue1);
      MessageReference ref2 = msg.createReference(queue2);
      MessageReference ref3 = msg.createReference(queue3);
      
      assertEquals(msg, ref1.getMessage());
      assertEquals(msg, ref2.getMessage());
      assertEquals(msg, ref3.getMessage());
      
      assertEquals(queue1, ref1.getQueue());
      assertEquals(queue2, ref2.getQueue());
      assertEquals(queue3, ref3.getQueue());
      
      EasyMock.verify(queue1, queue2, queue3);
      
      assertEquals(0, msg.getDurableRefCount());      
   }
   
   // Protected -----------------------------------------------------------------------------------
   
   protected void assertMessagesEquivalent(final Message msg1, final Message msg2)
   {
      super.assertMessagesEquivalent(msg1, msg2);
      
      ServerMessage smsg1 = (ServerMessage)msg1;
      ServerMessage smsg2 = (ServerMessage)msg2;
      
      assertEquals(smsg1.getConnectionID(), smsg2.getConnectionID());
      assertEquals(smsg1.getMessageID(), smsg2.getMessageID());
   }
}
