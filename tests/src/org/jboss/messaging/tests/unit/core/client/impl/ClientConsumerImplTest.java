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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerImpl;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

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
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);      
     
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
      }
            
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
            
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor, pd);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(numMessages, consumer.getBufferSize());         
   }
   
   private class DirectExecutorService extends AbstractExecutorService
   {
      public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
      {
         return false;
      }

      public boolean isShutdown()
      {
         return false;
      }

      public void shutdown()
      { 
      }

      public boolean isTerminated()
      {
         return false;
      }

      public List<Runnable> shutdownNow()
      {
         return null;
      }

      public void execute(Runnable command)
      {
         command.run();
      }
   }
   
   public void testHandleMessageWithNonDirectHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = new DirectExecutorService();
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
      MessageHandler handler = EasyMock.createStrictMock(MessageHandler.class);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
         
         EasyMock.expect(msg.isExpired()).andReturn(false);
         
         EasyMock.expect(msg.getDeliveryID()).andReturn((long)i);
         
         session.delivered((long)i, false);
         
         EasyMock.expect(msg.getEncodeSize()).andReturn(1);
         
         handler.onMessage(msg);
      }
            
      EasyMock.replay(session, connection, rc, pd, handler);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
      consumer.setMessageHandler(handler);
            
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, pd, handler);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(0, consumer.getBufferSize());         
   }
   
   public void testHandleMessageWithDirectHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
      MessageHandler handler = EasyMock.createStrictMock(MessageHandler.class);
      
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
            
      EasyMock.replay(session, connection, rc, executor, pd, handler);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, true, rc, pd, executor, 878787);
      
      consumer.setMessageHandler(handler);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor, pd, handler);      
      EasyMock.verify(msgs.toArray());
      
      assertEquals(0, consumer.getBufferSize());         
   }
     
   public void testSetGetHandlerWithMessagesAlreadyInBuffer() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
      MessageHandler handler = EasyMock.createStrictMock(MessageHandler.class);
      
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
         
         EasyMock.expect(msg.getPriority()).andReturn((byte)4); //default priority
      }
            
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }
      
      EasyMock.verify(session, connection, rc, executor, pd);      
      EasyMock.verify(msgs.toArray());
      
      EasyMock.reset(session, connection, rc, executor, pd);      
      EasyMock.reset(msgs.toArray());
      
      assertEquals(numMessages, consumer.getBufferSize());
      
      for (ClientMessage msg: msgs)
      {
         executor.execute(EasyMock.isA(Runnable.class));
      }
      
      EasyMock.replay(session, connection, rc, executor, pd, handler);
      EasyMock.replay(msgs.toArray());
      
      
      consumer.setMessageHandler(handler);
      
      EasyMock.verify(session, connection, rc, executor, pd, handler);
      EasyMock.verify(msgs.toArray());   
   }
   
   public void testReceiveNoTimeout() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
       
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
      
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
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
      
      EasyMock.verify(session, connection, rc, executor, pd);
      EasyMock.verify(msgs.toArray());
   }
   
   public void testReceiveWithTimeout() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
            
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
      
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
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
      
      EasyMock.verify(session, connection, rc, executor, pd);
      EasyMock.verify(msgs.toArray());
   }
   
   public void testReceiveImmediate() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
            
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
      
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
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
      
      EasyMock.verify(session, connection, rc, executor, pd);
      EasyMock.verify(msgs.toArray());
   }
   
   public void testReceiveExpired() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
            
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
      
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
      for (ClientMessage msg: msgs)
      {
         consumer.handleMessage(msg);
      }

      assertEquals(numMessages, consumer.getBufferSize());         

      for (ClientMessage msg: msgs)
      {
         assertNull(consumer.receiveImmediate());
      }
      
      EasyMock.verify(session, connection, rc, executor, pd);
      EasyMock.verify(msgs.toArray());
   }
   
   public void testReceiveWithHandler() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
             
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
      MessageHandler handler = new MessageHandler()
      {
         public void onMessage(ClientMessage msg)
         {            
         }
      };
      
      consumer.setMessageHandler(handler);
      
      try
      {
         consumer.receive();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.ILLEGAL_STATE, e.getCode());
      }      
   }
   

   public void testClose() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);

      final long clientTargetID = 120912;
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, clientTargetID, 787, false, rc, pd, executor, 878787);

      pd.unregister(clientTargetID);
      session.removeConsumer(consumer);
      EasyMock.expect(rc.sendBlocking(675765, 878787, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
      EasyMock.replay(session, connection, rc, executor, pd);
      
      consumer.close();
      EasyMock.verify(session, connection, rc, executor, pd);
      
      //Closing again should do nothing
      
      EasyMock.reset(session, connection, rc, executor, pd);
      EasyMock.replay(session, connection, rc, executor, pd);
      
      consumer.close();
      
      EasyMock.verify(session, connection, rc, executor, pd);      
   }
   
   
   public void testCleanUp() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);

      final long clientTargetID = 120912;
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, clientTargetID, 787, false, rc, pd, executor, 878787);

      pd.unregister(clientTargetID);
      session.removeConsumer(consumer);
      EasyMock.replay(session, connection, rc, executor, pd);
      consumer.cleanUp();
      EasyMock.verify(session, connection, rc, executor, pd);
   }
   
   public void testHandleOnRecover() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
       
      final int numMessages = 10;
      
      List<ClientMessage> msgs = new ArrayList<ClientMessage>();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
         
         msgs.add(msg);
      }
      
      //Handle the first five
      
      for (int i = 0; i < numMessages / 2; i++)                  
      {        
         ClientMessage msg = msgs.get(i);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority                            
      }
      
      //Then recover called so next 5 get rejected
      
      for (int i = numMessages / 2; i < numMessages; i++)                  
      {        
         ClientMessage msg = msgs.get(i);
         
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
      }
      
      //Then all ten get redelivered
      
      for (int i = 0; i < numMessages; i++)                  
      {        
         ClientMessage msg = msgs.get(i);
                  
         EasyMock.expect(msg.getDeliveryID()).andStubReturn((long)i);
         
         EasyMock.expect(msg.getPriority()).andStubReturn((byte)4); //default priority         
      }
      
      EasyMock.replay(session, connection, rc, executor, pd);
      EasyMock.replay(msgs.toArray());
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
            
      for (int i = 0; i < numMessages / 2; i++)                  
      {        
         ClientMessage msg = msgs.get(i);
      
         consumer.handleMessage(msg);
      }
      
      assertEquals(numMessages / 2, consumer.getBufferSize());
      
      consumer.recover(0);
      
      assertEquals(0, consumer.getBufferSize());
               
      for (int i = numMessages / 2; i < numMessages; i++)
      {      
         ClientMessage msg = msgs.get(i);
   
         consumer.handleMessage(msg);
      }
      
      assertEquals(0, consumer.getBufferSize());
      
      for (int i = 0; i < numMessages; i++)                  
      {        
         ClientMessage msg = msgs.get(i);
      
         consumer.handleMessage(msg);
      }
      
      assertEquals(numMessages, consumer.getBufferSize()); 
      
      EasyMock.verify(session, connection, rc, executor, pd);
      EasyMock.verify(msgs.toArray());      
   }
   
   public void testHandleOnClose() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      
      ClientMessage msg1 = EasyMock.createStrictMock(ClientMessage.class);
      ClientMessage msg2 = EasyMock.createStrictMock(ClientMessage.class);
      
      final long clientTargetID = 120912;
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, clientTargetID, 787, false, rc, pd, executor, 878787);

      pd.unregister(clientTargetID);
      session.removeConsumer(consumer);
      EasyMock.expect(rc.sendBlocking(675765, 878787, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
      EasyMock.replay(session, connection, rc, executor, pd, msg1, msg2);
      
      consumer.close();
            
      consumer.handleMessage(msg1);
      consumer.handleMessage(msg2);
      
      EasyMock.verify(session, connection, rc, executor, pd, msg1, msg2);
      
      assertEquals(0, consumer.getBufferSize());
   }
   
   // Private -----------------------------------------------------------------------------------------------------------
   
   private void testConstructor(final long targetID, final long clientTargetID,
                                final int windowSize, final boolean direct) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);      
      final long sessionTargetID = 102912;
      
      EasyMock.replay(session, connection, rc, executor, pd);
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, targetID, clientTargetID, windowSize, direct, rc, pd, executor, sessionTargetID);
      
      EasyMock.verify(session, connection, rc, executor, pd);
      
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
