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
package org.jboss.messaging.tests.unit.core.remoting.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A RemotingHandlerImplTest
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RemotingHandlerImplTest extends UnitTestCase
{
   private RemotingHandlerImpl handler;
   private MessagingBuffer buff;
   private PacketDispatcher dispatcher;
   private ExecutorService executorService;

   protected void setUp() throws Exception
   {
      super.setUp();
      dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      executorService = EasyMock.createStrictMock(ExecutorService.class);
      handler = new RemotingHandlerImpl(dispatcher, executorService);
      buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      handler = null;
      buff = null;
   }
   
   public void testIsReadyToHandleLessThanInt()
   {
      buff.putByte((byte)8);
      buff.flip();
      assertEquals(-1, handler.isReadyToHandle(buff));
   }
   
   public void testIsReadyToHandleLessThanLength()
   {
      buff.putInt((byte)100);
      buff.putInt(125412);
      buff.flip();
      assertEquals(-1, handler.isReadyToHandle(buff));
   }
      
   public void testIsReadyToHandleEqualsLength()
   {
      buff.putInt((byte)100);
      for (int i = 0; i < 100; i++)
      {
         buff.putByte((byte)123);
      }
      buff.flip();
      assertEquals(100, handler.isReadyToHandle(buff));
   }
   
   public void testIsReadyToHandleMoreThanLength()
   {
      buff.putInt((byte)100);
      for (int i = 0; i < 200; i++)
      {
         buff.putByte((byte)123);
      }
      buff.flip();
      assertEquals(100, handler.isReadyToHandle(buff));
   }
   
   public void testBufferReceivedNoExecutor() throws Exception
   {
      handler = new RemotingHandlerImpl(dispatcher, null);
      
      Packet packet = new PacketImpl(PacketImpl.CLOSE);
      
      final long connectionID = 123123;
      final int numTimes = 10;
      
      dispatcher.dispatch(connectionID, packet);
      EasyMock.expectLastCall().times(numTimes);
      
      packet.encode(buff);
      buff.getInt();
      
      EasyMock.replay(dispatcher);
      
      for (int i = 0; i < numTimes; i++)
      {
         handler.bufferReceived(connectionID, buff);
         
         buff.flip();
         buff.getInt();
      }
      
      EasyMock.verify(dispatcher);
   }
   
   public void testBufferReceivedWithExecutor() throws Exception
   {
      Packet packet = new PacketImpl(PacketImpl.CLOSE);
      
      final long connectionID = 123123;
      final int numTimes = 10;
      
      this.executorService.execute(EasyMock.isA(Runnable.class));
      EasyMock.expectLastCall().times(numTimes);
      
      packet.encode(buff);
      buff.getInt();
      
      EasyMock.replay(dispatcher);
      
      for (int i = 0; i < numTimes; i++)
      {
         handler.bufferReceived(connectionID, buff);
         
         buff.flip();
         buff.getInt();
      }
      
      EasyMock.verify(dispatcher);
   }
   
   public void testExecutors() throws Exception
   {            
      final long connectionID = 123123;
      final int numTimes = 10;
      
      this.executorService.execute(EasyMock.isA(Runnable.class));
      EasyMock.expectLastCall().times(numTimes);
                  
      EasyMock.replay(dispatcher);
      
      for (int i = 0; i < numTimes; i++)
      {
         Packet packet = new PacketImpl(PacketImpl.CLOSE);
         packet.setExecutorID(i);
         MessagingBuffer buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
         packet.encode(buff);
         buff.getInt();
                  
         handler.bufferReceived(connectionID, buff);
      }
      
      assertEquals(numTimes, handler.getNumExecutors());
      
      EasyMock.verify(dispatcher);
      
      for (int i = 0; i < numTimes; i++)
      {
         handler.closeExecutor(i);
      }
      
      assertEquals(0, handler.getNumExecutors());      
   }
         
   // Packet tests

   public void testInvalidPacketType() throws Exception
   {
      buff = new ByteBufferWrapper(ByteBuffer.allocateDirect(30));
      buff.putInt(26);
      buff.putInt(Integer.MAX_VALUE);
      try
      {
         PacketImpl copy = (PacketImpl) handler.decode(123, buff);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }


   public void testEmptyPacket() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.NULL);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketClose() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.CLOSE);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketConnStart() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_START);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketConnStop() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_STOP);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessRecover() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_RECOVER);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessCommit() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_COMMIT);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessRollback() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_ROLLBACK);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessBrowserReset() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_BROWSER_RESET);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessBrowserHasNextMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessBrowserNextMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessXaInDoubtXids() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessXaGetTimeout() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testEmptyPacketSessXaSuspend() throws Exception
   {
      PacketImpl message = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      PacketImpl copy = (PacketImpl) handler.decode(123, buff);
      checkHeaders(message, copy);
   }

   public void testProducerSendMessageNullBodyNoProps1() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomInt(), RandomUtil.randomLong());
      message1.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
   }

   public void testProducerSendMessageNullBodyNoProps2() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(),
              RandomUtil.randomLong(), RandomUtil.randomLong(), RandomUtil.randomByte(), new ByteBufferWrapper(ByteBuffer.allocate(0)));
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
   }

   public void testProducerSendMessageNullBodyNoProps3() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(), new ByteBufferWrapper(ByteBuffer.allocate(0)));
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
   }


   public void testProducerSendMessageNullBodyNoProps4() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomBoolean(), new ByteBufferWrapper(ByteBuffer.allocate(0)));
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
   }

   public void testProducerSendMessageBodyProps1() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomInt(), RandomUtil.randomLong());
      message1.setDestination(new SimpleString("test"));
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
      assertEquals(9, copy.getServerMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getServerMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getServerMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getServerMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getServerMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getServerMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getServerMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getServerMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getServerMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getServerMessage().getProperty(stringProp));
   }

   public void testProducerSendMessageBodyProps2() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(),
              RandomUtil.randomLong(), RandomUtil.randomLong(), RandomUtil.randomByte(), new ByteBufferWrapper(ByteBuffer.allocate(1024)));
      message1.setDestination(new SimpleString("test"));
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
      assertEquals(9, copy.getServerMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getServerMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getServerMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getServerMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getServerMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getServerMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getServerMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getServerMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getServerMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getServerMessage().getProperty(stringProp));
   }

   public void testProducerSendMessageBodyProps3() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(), new ByteBufferWrapper(ByteBuffer.allocate(1024)));
      message1.setDestination(new SimpleString("test"));
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
      assertEquals(9, copy.getServerMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getServerMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getServerMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getServerMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getServerMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getServerMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getServerMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getServerMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getServerMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getServerMessage().getProperty(stringProp));
   }


   public void testProducerSendMessageBodyProps4() throws Exception
   {
      ClientMessageImpl message1 = new ClientMessageImpl(RandomUtil.randomBoolean(), new ByteBufferWrapper(ByteBuffer.allocate(1024)));
      message1.setDestination(new SimpleString("test"));
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      PacketImpl message = new ProducerSendMessage(message1);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ProducerSendMessage copy = (ProducerSendMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getServerMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getServerMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getServerMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getServerMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getServerMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getServerMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getServerMessage().getType());
      assertEquals(9, copy.getServerMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getServerMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getServerMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getServerMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getServerMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getServerMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getServerMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getServerMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getServerMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getServerMessage().getProperty(stringProp));
   }

   public void testProducerReceiveMessageNullBodyNoProps1() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl();
      message1.setDestination(new SimpleString("test"));
      message1.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
   }

   public void testProducerReceiveMessageNullBodyNoProps2() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(RandomUtil.randomLong());
      message1.setDestination(new SimpleString("test"));
      message1.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
   }

   public void testProducerReceiveMessageNullBodyNoProps3() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(new ServerMessageImpl());
      message1.setDestination(new SimpleString("test"));
      message1.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
   }

   public void testProducerReceiveMessageNullBodyNoProps4() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(), RandomUtil.randomLong(),
              RandomUtil.randomLong(), RandomUtil.randomByte(),new ByteBufferWrapper(ByteBuffer.allocateDirect(1024)));
      message1.setDestination(new SimpleString("test"));
      message1.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(0, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
   }


   public void testProducerReceiveMessageBodyProps1() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl();
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
      assertEquals(9, copy.getClientMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getClientMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getClientMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getClientMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getClientMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getClientMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getClientMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getClientMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getClientMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getClientMessage().getProperty(stringProp));
   }

   public void testProducerReceiveMessageBodyProps2() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(RandomUtil.randomLong());
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
      assertEquals(9, copy.getClientMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getClientMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getClientMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getClientMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getClientMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getClientMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getClientMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getClientMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getClientMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getClientMessage().getProperty(stringProp));
   }

   public void testProducerReceiveMessageBodyProps3() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(new ServerMessageImpl());
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
      assertEquals(9, copy.getClientMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getClientMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getClientMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getClientMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getClientMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getClientMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getClientMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getClientMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getClientMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getClientMessage().getProperty(stringProp));
   }

   public void testProducerReceiveMessageBodyProps4() throws Exception
   {
      ServerMessage message1 = new ServerMessageImpl(RandomUtil.randomByte(), RandomUtil.randomBoolean(), RandomUtil.randomLong(),
              RandomUtil.randomLong(), RandomUtil.randomByte(), new ByteBufferWrapper(ByteBuffer.allocateDirect(1024)));
      message1.setDestination(new SimpleString("test"));
      PacketImpl message = new ReceiveMessage(message1, RandomUtil.randomInt(), RandomUtil.randomInt());
      byte[] bytes = RandomUtil.randomBytes();
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocateDirect(bytes.length));
      body.putBytes(bytes);
      body.rewind();
      message1.setBody(body);
      setHeaders(message);
      SimpleString boolProp = new SimpleString("bool.prop");
      message1.putBooleanProperty(boolProp, RandomUtil.randomBoolean());
      SimpleString byteProp = new SimpleString("byte.prop");
      message1.putByteProperty(byteProp, RandomUtil.randomByte());
      SimpleString byteprops = new SimpleString("bytes.prop");
      message1.putBytesProperty(byteprops, RandomUtil.randomBytes());
      SimpleString doubleProp = new SimpleString("double.prop");
      message1.putDoubleProperty(doubleProp, RandomUtil.randomDouble());
      SimpleString floatProp = new SimpleString("float.prop");
      message1.putFloatProperty(floatProp, RandomUtil.randomFloat());
      SimpleString intProp = new SimpleString("int.prop");
      message1.putIntProperty(intProp, RandomUtil.randomInt());
      SimpleString longProp = new SimpleString("long.prop");
      message1.putLongProperty(longProp, RandomUtil.randomLong());
      SimpleString shortProp = new SimpleString("short.prop");
      message1.putShortProperty(shortProp, RandomUtil.randomShort());
      SimpleString stringProp = new SimpleString("string.prop");
      message1.putStringProperty(stringProp, RandomUtil.randomSimpleString());
      message.encode(buff);
      buff.getInt();
      ReceiveMessage copy = (ReceiveMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(bytes.length, copy.getClientMessage().getBody().limit());
      assertEquals(message1.getDestination(), copy.getClientMessage().getDestination());
      assertEquals(message1.getEncodeSize(), copy.getClientMessage().getEncodeSize());
      assertEquals(message1.getExpiration(), copy.getClientMessage().getExpiration());
      assertEquals(message1.getPriority(), copy.getClientMessage().getPriority());
      assertEquals(message1.getTimestamp(), copy.getClientMessage().getTimestamp());
      assertEquals(message1.getType(), copy.getClientMessage().getType());
      assertEquals(9, copy.getClientMessage().getPropertyNames().size());
      assertEquals(message1.getProperty(boolProp), copy.getClientMessage().getProperty(boolProp));
      assertEquals(message1.getProperty(byteProp), copy.getClientMessage().getProperty(byteProp));
      assertEqualsByteArrays((byte[]) message1.getProperty(byteprops), (byte[]) copy.getClientMessage().getProperty(byteprops));
      assertEquals(message1.getProperty(doubleProp), copy.getClientMessage().getProperty(doubleProp));
      assertEquals(message1.getProperty(floatProp), copy.getClientMessage().getProperty(floatProp));
      assertEquals(message1.getProperty(intProp), copy.getClientMessage().getProperty(intProp));
      assertEquals(message1.getProperty(longProp), copy.getClientMessage().getProperty(longProp));
      assertEquals(message1.getProperty(shortProp), copy.getClientMessage().getProperty(shortProp));
      assertEquals(message1.getProperty(stringProp), copy.getClientMessage().getProperty(stringProp));
   }

   public void testCreateSessionPacket() throws Exception
   {
      CreateSessionMessage message = new CreateSessionMessage(RandomUtil.randomString(), RandomUtil.randomInt(),
               RandomUtil.randomString(), RandomUtil.randomString(),
               RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(),
               RandomUtil.randomLong());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      CreateSessionMessage copy = (CreateSessionMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getName(), copy.getName());
      assertEquals(message.getVersion(), copy.getVersion());
      assertEquals(message.getUsername(), copy.getUsername());
      assertEquals(message.getPassword(), copy.getPassword());
      assertEquals(message.isAutoCommitAcks(), copy.isAutoCommitAcks());
      assertEquals(message.isAutoCommitSends(), copy.isAutoCommitSends());
      assertEquals(message.isXA(), copy.isXA());
      assertEquals(message.getResponseTargetID(), copy.getResponseTargetID());
   }
   
   public void testCreateSessionPacketNullUsernamePassword() throws Exception
   {
      CreateSessionMessage message = new CreateSessionMessage(RandomUtil.randomString(), RandomUtil.randomInt(),
               null, null,
               RandomUtil.randomBoolean(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(),
               RandomUtil.randomLong());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      CreateSessionMessage copy = (CreateSessionMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getName(), copy.getName());
      assertEquals(message.getVersion(), copy.getVersion());
      assertNull(message.getUsername());
      assertNull(message.getPassword());
      assertEquals(message.isAutoCommitAcks(), copy.isAutoCommitAcks());
      assertEquals(message.isAutoCommitSends(), copy.isAutoCommitSends());
      assertEquals(message.isXA(), copy.isXA());
      assertEquals(message.getResponseTargetID(), copy.getResponseTargetID());
   }

   public void testConsumerFlowCreditMessagePacket() throws Exception
   {
      ConsumerFlowCreditMessage message = new ConsumerFlowCreditMessage(RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ConsumerFlowCreditMessage copy = (ConsumerFlowCreditMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getTokens(), copy.getTokens());
   }

   public void testCreateSessionResponseMessagePacket() throws Exception
   {
      CreateSessionResponseMessage message =
         new CreateSessionResponseMessage(RandomUtil.randomLong(), RandomUtil.randomLong(),
                  RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      CreateSessionResponseMessage copy = (CreateSessionResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getSessionID(), copy.getSessionID());
      assertEquals(message.getCommandResponseTargetID(), copy.getCommandResponseTargetID());
      assertEquals(message.getServerVersion(), copy.getServerVersion());
   }

   public void testMessagingExceptionMessagePacket() throws Exception
   {
      MessagingExceptionMessage message = new MessagingExceptionMessage(new MessagingException(RandomUtil.randomInt()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      MessagingExceptionMessage copy = (MessagingExceptionMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getException().getCode(), copy.getException().getCode());
   }

   public void testProducerFlowCreditMessagePacket() throws Exception
   {
      ProducerFlowCreditMessage message = new ProducerFlowCreditMessage(RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      ProducerFlowCreditMessage copy = (ProducerFlowCreditMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getTokens(), copy.getTokens());
   }

   public void testSessionAcknowledgeMessagePacket() throws Exception
   {
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(RandomUtil.randomLong(), RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionAcknowledgeMessage copy = (SessionAcknowledgeMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getDeliveryID(), copy.getDeliveryID());
      assertEquals(message.isAllUpTo(), copy.isAllUpTo());
   }

   public void testSessionAddDestinationMessagePacket() throws Exception
   {
      SessionAddDestinationMessage message =
         new SessionAddDestinationMessage(RandomUtil.randomSimpleString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionAddDestinationMessage copy = (SessionAddDestinationMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.isDurable(), copy.isDurable());
      assertEquals(message.isTemporary(), copy.isTemporary());
   }

   public void testSessionBindingQueryMessagePacket() throws Exception
   {
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionBindingQueryMessage copy = (SessionBindingQueryMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
   }

   public void testSessionBindingQueryResponseMessagePacket() throws Exception
   {

      ArrayList<SimpleString> list = new ArrayList<SimpleString>();
      list.add(RandomUtil.randomSimpleString());
      SessionBindingQueryResponseMessage message = new SessionBindingQueryResponseMessage(RandomUtil.randomBoolean(), list);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionBindingQueryResponseMessage copy = (SessionBindingQueryResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.isExists(), copy.isExists());
      assertEquals(message.getQueueNames().get(0), copy.getQueueNames().get(0));
      assertEquals(1, message.getQueueNames().size());

   }

   public void testSessionBrowserHasNextMessageResponseMessagePacket() throws Exception
   {
      SessionBrowserHasNextMessageResponseMessage message = new SessionBrowserHasNextMessageResponseMessage(RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionBrowserHasNextMessageResponseMessage copy = (SessionBrowserHasNextMessageResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.hasNext(), copy.hasNext());

   }

   public void testSessionCancelMessagePacket() throws Exception
   {
      SessionCancelMessage message = new SessionCancelMessage(RandomUtil.randomLong(), RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCancelMessage copy = (SessionCancelMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getDeliveryID(), copy.getDeliveryID());
      assertEquals(message.isExpired(), copy.isExpired());

   }

   public void testSessionCreateBrowserMessagePacket() throws Exception
   {
      SessionCreateBrowserMessage message = new SessionCreateBrowserMessage(RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateBrowserMessage copy = (SessionCreateBrowserMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getQueueName(), copy.getQueueName());

   }

   public void testSessionCreateBrowserResponseMessagePacket() throws Exception
   {
      SessionCreateBrowserResponseMessage message = new SessionCreateBrowserResponseMessage(RandomUtil.randomLong());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateBrowserResponseMessage copy = (SessionCreateBrowserResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getBrowserTargetID(), copy.getBrowserTargetID());

   }

   public void testSessionCreateConsumerMessagePacket() throws Exception
   {
      SessionCreateConsumerMessage message = new SessionCreateConsumerMessage(RandomUtil.randomLong(),
              RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString(),
              RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateConsumerMessage copy = (SessionCreateConsumerMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getClientTargetID(), copy.getClientTargetID());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getMaxRate(), copy.getMaxRate());
      assertEquals(message.getQueueName(), copy.getQueueName());
      assertEquals(message.getWindowSize(), copy.getWindowSize());
   }

   public void testSessionCreateConsumerResponseMessagePacket() throws Exception
   {
      SessionCreateConsumerResponseMessage message = new SessionCreateConsumerResponseMessage(RandomUtil.randomLong(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateConsumerResponseMessage copy = (SessionCreateConsumerResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getConsumerTargetID(), copy.getConsumerTargetID());
      assertEquals(message.getWindowSize(), copy.getWindowSize());

   }

   public void testSessionCreateProducerMessagePacket() throws Exception
   {
      SessionCreateProducerMessage message = new SessionCreateProducerMessage(RandomUtil.randomLong(),
              RandomUtil.randomSimpleString(), RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateProducerMessage copy = (SessionCreateProducerMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getWindowSize(), copy.getWindowSize());
      assertEquals(message.getClientTargetID(), copy.getClientTargetID());
      assertEquals(message.getMaxRate(), copy.getMaxRate());

   }

   public void testSessionCreateProducerResponseMessagePacket() throws Exception
   {
      SessionCreateProducerResponseMessage message = new SessionCreateProducerResponseMessage(RandomUtil.randomLong(), RandomUtil.randomInt(),
              RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateProducerResponseMessage copy = (SessionCreateProducerResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getInitialCredits(), copy.getInitialCredits());
      assertEquals(message.getProducerTargetID(), copy.getProducerTargetID());
      assertEquals(message.getMaxRate(), copy.getMaxRate());

   }

   public void testSessionCreateQueueMessagePacket() throws Exception
   {
      SessionCreateQueueMessage message = new SessionCreateQueueMessage(RandomUtil.randomSimpleString(),
              RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionCreateQueueMessage copy = (SessionCreateQueueMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getQueueName(), copy.getQueueName());
      assertEquals(message.isDurable(), copy.isDurable());
      assertEquals(message.isDurable(), copy.isDurable());
   }

   public void testSessionDeleteQueueMessagePacket() throws Exception
   {
      SessionDeleteQueueMessage message = new SessionDeleteQueueMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionDeleteQueueMessage copy = (SessionDeleteQueueMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getQueueName(), copy.getQueueName());
   }

   public void testSessionQueueQueryMessagePacket() throws Exception
   {
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionQueueQueryMessage copy = (SessionQueueQueryMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getQueueName(), copy.getQueueName());
   }
 
   public void testSessionQueueQueryResponseMessagePacket() throws Exception
   {
      SessionQueueQueryResponseMessage message =
         new SessionQueueQueryResponseMessage(RandomUtil.randomBoolean(),
              RandomUtil.randomInt(), RandomUtil.randomInt(), RandomUtil.randomInt(),
              RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionQueueQueryResponseMessage copy = (SessionQueueQueryResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getConsumerCount(), copy.getConsumerCount());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getMaxSize(), copy.getMaxSize());
      assertEquals(message.getMessageCount(), copy.getMessageCount());
      assertEquals(message.isDurable(), copy.isDurable());
      assertEquals(message.isExists(), copy.isExists());
      assertEquals(message.isDurable(), copy.isDurable());
   }

   public void testSessionRemoveDestinationMessagePacket() throws Exception
   {
      SessionRemoveDestinationMessage message = new SessionRemoveDestinationMessage(RandomUtil.randomSimpleString(), RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionRemoveDestinationMessage copy = (SessionRemoveDestinationMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.isDurable(), copy.isDurable());
   }

   public void testSessionXACommitMessagePacket() throws Exception
   {
      SessionXACommitMessage message = new SessionXACommitMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()),
              RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXACommitMessage copy = (SessionXACommitMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.isOnePhase(), copy.isOnePhase());
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAEndMessagePacket() throws Exception
   {
      SessionXAEndMessage message = new SessionXAEndMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()),
              RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAEndMessage copy = (SessionXAEndMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.isFailed(), copy.isFailed());
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAForgetMessagePacket() throws Exception
   {
      SessionXAForgetMessage message = new SessionXAForgetMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAForgetMessage copy = (SessionXAForgetMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAGetInDoubtXidsResponseMessagePacket() throws Exception
   {
      XidImpl xids = new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes());
      ArrayList<Xid> list = new ArrayList<Xid>();
      SessionXAGetInDoubtXidsResponseMessage message = new SessionXAGetInDoubtXidsResponseMessage(list);
      list.add(xids);
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAGetInDoubtXidsResponseMessage copy = (SessionXAGetInDoubtXidsResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXids().get(0).getBranchQualifier(), copy.getXids().get(0).getBranchQualifier());
      assertEquals(message.getXids().get(0).getFormatId(), copy.getXids().get(0).getFormatId());
      assertEqualsByteArrays(message.getXids().get(0).getGlobalTransactionId(), copy.getXids().get(0).getGlobalTransactionId());
      assertEquals(1, message.getXids().size());
   }

   public void testSessionXAGetTimeoutResponseMessagePacket() throws Exception
   {
      SessionXAGetTimeoutResponseMessage message = new SessionXAGetTimeoutResponseMessage(RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAGetTimeoutResponseMessage copy = (SessionXAGetTimeoutResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getTimeoutSeconds(), copy.getTimeoutSeconds());
   }

   public void testSessionXAJoinMessagePacket() throws Exception
   {
      SessionXAJoinMessage message = new SessionXAJoinMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAJoinMessage copy = (SessionXAJoinMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAPrepareMessagePacket() throws Exception
   {
      SessionXAPrepareMessage message = new SessionXAPrepareMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAPrepareMessage copy = (SessionXAPrepareMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAResponseMessagePacket() throws Exception
   {
      SessionXAResponseMessage message = new SessionXAResponseMessage(RandomUtil.randomBoolean(), RandomUtil.randomInt(), RandomUtil.randomString());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAResponseMessage copy = (SessionXAResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getMessage(), copy.getMessage());
      assertEquals(message.getResponseCode(), copy.getResponseCode());
      assertEquals(message.isError(), copy.isError());
   }

   public void testSessionXAResumeMessagePacket() throws Exception
   {
      SessionXAResumeMessage message = new SessionXAResumeMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAResumeMessage copy = (SessionXAResumeMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXARollbackMessagePacket() throws Exception
   {
      SessionXARollbackMessage message = new SessionXARollbackMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXARollbackMessage copy = (SessionXARollbackMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXASetTimeoutMessagePacket() throws Exception
   {
      SessionXASetTimeoutMessage message = new SessionXASetTimeoutMessage(RandomUtil.randomInt());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXASetTimeoutMessage copy = (SessionXASetTimeoutMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.getTimeoutSeconds(), copy.getTimeoutSeconds());
   }

   public void testSessionXASetTimeoutResponseMessagePacket() throws Exception
   {
      SessionXASetTimeoutResponseMessage message = new SessionXASetTimeoutResponseMessage(RandomUtil.randomBoolean());
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXASetTimeoutResponseMessage copy = (SessionXASetTimeoutResponseMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEquals(message.isOK(), copy.isOK());
   }

   public void testSessionXAStartMessagePacket() throws Exception
   {
      SessionXAStartMessage message = new SessionXAStartMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      message.encode(buff);
      buff.getInt();
      SessionXAStartMessage copy = (SessionXAStartMessage) handler.decode(123, buff);
      checkHeaders(message, copy);
      assertEqualsByteArrays(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertEqualsByteArrays(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   private void setHeaders(PacketImpl packet)
   {
      packet.setExecutorID(RandomUtil.randomLong());
      packet.setResponseTargetID(RandomUtil.randomLong());
      packet.setTargetID(RandomUtil.randomLong());
   }

   private void checkHeaders(PacketImpl emptyPacket, PacketImpl emptyPacket2)
   {
      assertEquals(emptyPacket.getExecutorID(), emptyPacket2.getExecutorID());
      assertEquals(emptyPacket.getResponseTargetID(), emptyPacket2.getResponseTargetID());
      assertEquals(emptyPacket.getTargetID(), emptyPacket2.getTargetID());
   }
}
