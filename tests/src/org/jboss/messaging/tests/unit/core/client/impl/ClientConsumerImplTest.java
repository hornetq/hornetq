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

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerImpl;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 
 * A ClientConsumerImplTest
 * 
 * TODO - still need to test:
 * priority
 * flow control
 * closing
 * waiting for message listener to complete etc
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConsumerImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientConsumerImplTest.class);

   public void testConstructor() throws Exception
   {
      testConstructor(6565, 71627162, 7676, false);
      testConstructor(6565, 71627162, 7676, true);
      testConstructor(6565, 71627162, -1, false);
      testConstructor(6565, 71627162, -1, true);
   }
   
   
   public void testHandleMessageNoHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      

      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
      }
            
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(numMessages, consumer.getBufferSize());         
   }
   
   public void testHandleMessageWithNonDirectHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      

      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
         
         executor.execute(EasyMock.isA(Runnable.class));
      }
            
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {            
         }
      });
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(numMessages, consumer.getBufferSize());         
   }
   
   public void testHandleMessageWithDirectHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      MessageHandler handler = EasyMock.createStrictMock(MessageHandler.class);
      
      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.isExpired()).andReturn(false);
         
         EasyMock.expect(msg.getDeliveryID()).andReturn((long)i);
         
         session.delivered((long)i, false);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
         
         handler.onMessage(msg);
      }
            
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, true);
      
      consumer.setMessageHandler(handler);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(0, consumer.getBufferSize());         
   }
     
   public void testSetGetHandlerWithMessagesAlreadyInBuffer() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
      }
            
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor);      
      EasyMock.verify(msgs.toArray());
      
      EasyMock.reset(session, connection, rc, executor);      
      EasyMock.reset(msgs.toArray());
      
      assertEquals(numMessages, consumer.getBufferSize());
      
      for (ClientMessage msg: msgs)
      {
         executor.execute(EasyMock.isA(Runnable.class));
      }
      
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
      
      MessageHandler handler = EasyMock.createStrictMock(MessageHandler.class);
      
      consumer.setMessageHandler(handler);
      
      EasyMock.verify(session, connection, rc, executor);
      EasyMock.verify(msgs.toArray());   
   }
   
   public void testReceiveNoTimeout() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andStubReturn(executor);
      EasyMock.expect(session.getConnection()).andStubReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andStubReturn(rc);
      
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
         
         EasyMock.expect(msg.isExpired()).andStubReturn(false);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         session.delivered((long)i, false);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
      }
      
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }

      assertEquals(numMessages, consumer.getBufferSize());         

      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage msg = consumer.receive();
   
         assertTrue(msg == msgs.get(i));
      }

      assertNull(consumer.receiveImmediate());      
   }
   
   public void testReceiveWithTimeout() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andStubReturn(executor);
      EasyMock.expect(session.getConnection()).andStubReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andStubReturn(rc);
      
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
         
         EasyMock.expect(msg.isExpired()).andStubReturn(false);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         session.delivered((long)i, false);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
      }
      
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }

      assertEquals(numMessages, consumer.getBufferSize());         

      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage msg = consumer.receive(1000);
   
         assertTrue(msg == msgs.get(i));
      }

      assertNull(consumer.receiveImmediate());           
   }
   
   public void testReceiveImmediate() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andStubReturn(executor);
      EasyMock.expect(session.getConnection()).andStubReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andStubReturn(rc);
      
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
         
         EasyMock.expect(msg.isExpired()).andStubReturn(false);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         session.delivered((long)i, false);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
      }
      
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }

      assertEquals(numMessages, consumer.getBufferSize());         

      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage msg = consumer.receiveImmediate();
   
         assertTrue(msg == msgs.get(i));
      }

      assertNull(consumer.receiveImmediate());           
   }
   
   public void testReceiveExpired() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andStubReturn(executor);
      EasyMock.expect(session.getConnection()).andStubReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andStubReturn(rc);
            
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority
         
         EasyMock.expect(msg.isExpired()).andStubReturn(true);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         session.delivered((long)i, true);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
      }
      
      EasyMock.replay(session, connection, rc, executor);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 54545, 54544, 545454, false);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }

      assertEquals(numMessages, consumer.getBufferSize());         

      assertNull(consumer.receiveImmediate());           
   }

   public void testCleanUp() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);

      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);

      EasyMock.replay(session, connection, rc);

      ClientConsumerInternal consumer =
      new ClientConsumerImpl(session, 1, 2, 3, true);

      EasyMock.verify(session, connection, rc);

      EasyMock.reset(session, connection, rc);
      PacketDispatcher packetDispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      EasyMock.expect(rc.getPacketDispatcher()).andReturn(packetDispatcher);
      packetDispatcher.unregister(2);
      session.removeConsumer(consumer);
      EasyMock.replay(session, connection, rc, packetDispatcher);
      consumer.cleanUp();
      EasyMock.verify(session, connection, rc, packetDispatcher);
   }
   // Private -----------------------------------------------------------------------------------------------------------

   
   private void testConstructor(final long targetID, final long clientTargetID,
         final int windowSize, final boolean direct) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      
      EasyMock.expect(session.getExecutorService()).andReturn(executor);
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      EasyMock.replay(session, connection, rc);
      
      ClientConsumerInternal consumer =
      new ClientConsumerImpl(session, targetID, clientTargetID, windowSize, direct);
      
      EasyMock.verify(session, connection, rc);
      
      assertEquals(direct, consumer.isDirect());
      assertFalse(consumer.isClosed());
      assertEquals(clientTargetID, consumer.getClientTargetID());
      assertEquals(windowSize, consumer.getClientWindowSize());
      assertEquals(0, consumer.getBufferSize());
      assertEquals(-1, consumer.getIgnoreDeliveryMark());
      assertEquals(0, consumer.getCreditsToSend());
      assertNull(consumer.getMessageHandler());
   }

}
