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

import static org.easymock.EasyMock.getCurrentArguments;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A RemotingConnectionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RemotingConnectionImplTest extends UnitTestCase
{
   public void testGetID() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      
      final long id = 12123;
      EasyMock.expect(connection.getID()).andReturn(id);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      EasyMock.replay(connection, dispatcher);
      
      assertEquals(id, rc.getID());
      
      EasyMock.verify(connection, dispatcher);     
   }
   
   public void testSendOneWay1() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      
      final long targetID = 12908783;
      final long executorID = 36464;
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);
      
      MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
      
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andReturn(buff);
      
      packet.encode(buff);
      
      connection.write(buff);
            
      EasyMock.replay(connection, dispatcher, packet, buff);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      rc.sendOneWay(targetID, executorID, packet);
      
      EasyMock.verify(connection, dispatcher, packet, buff);     
   }
   
   public void testSendOneWay2() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      Packet packet = EasyMock.createStrictMock(Packet.class);
            
      MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
      
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andReturn(buff);
      
      packet.encode(buff);
      
      connection.write(buff);
            
      EasyMock.replay(connection, dispatcher, packet, buff);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      rc.sendOneWay(packet);
      
      EasyMock.verify(connection, dispatcher, packet, buff);     
   }
   
   public void testFailureListeners() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
               
      connection.close();
      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      class Listener implements FailureListener
      {
         volatile MessagingException me;
         public void connectionFailed(MessagingException me)
         {
            this.me = me;
         }
      }
      
      Listener listener1 = new Listener();
      Listener listener2 = new Listener();
      Listener listener3 = new Listener();
      
      rc.addFailureListener(listener1);
      rc.addFailureListener(listener2);
      rc.addFailureListener(listener3);
      
      MessagingException me = new MessagingException(123213, "bjasajs");
      
      rc.fail(me);
      
      assertTrue(listener1.me == me);
      assertTrue(listener2.me == me);
      assertTrue(listener3.me == me);
      
      rc.removeFailureListener(listener2);
      
      MessagingException me2 = new MessagingException(2828, "uhuhuihuh");
      
      rc.fail(me2);
      
      assertTrue(listener1.me == me2);
      assertFalse(listener2.me == me2);
      assertTrue(listener3.me == me2);
                        
      EasyMock.verify(connection, dispatcher);  
      
      rc.removeFailureListener(listener1);
      rc.removeFailureListener(listener3);
      
      MessagingException me3 = new MessagingException(239565, "uhuhuihsdfsdfuh");
      
      rc.fail(me3);
      
      assertFalse(listener1.me == me3);
      assertFalse(listener2.me == me3);
      assertFalse(listener3.me == me3);
      
   }
   
   /*
    * Throwing exception from inside listener shouldn't prevent others from being run
    */
   public void testThrowExceptionFromFailureListener() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
               
      connection.close();
      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      class Listener implements FailureListener
      {
         volatile MessagingException me;
         public void connectionFailed(MessagingException me)
         {
            this.me = me;
            throw new RuntimeException();
         }
      }
      
      Listener listener1 = new Listener();
      Listener listener2 = new Listener();
      Listener listener3 = new Listener();
      
      rc.addFailureListener(listener1);
      rc.addFailureListener(listener2);
      rc.addFailureListener(listener3);
      
      MessagingException me = new MessagingException(123213, "bjasajs");
      
      rc.fail(me);
      
      assertTrue(listener1.me == me);
      assertTrue(listener2.me == me);
      assertTrue(listener3.me == me);  
   }
   
   public void testCreateBuffer() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      
      MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
      
      final int size = 1234;
            
      EasyMock.expect(connection.createBuffer(size)).andReturn(buff);
      
      EasyMock.replay(connection, dispatcher, buff);      
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      MessagingBuffer buff2 = rc.createBuffer(size);
      
      assertTrue(buff == buff2);       
   }
   
   public void testGetLocation() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      assertTrue(location == rc.getLocation());       
   }
   
   public void testGetPacketDispatcher() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      assertTrue(dispatcher == rc.getPacketDispatcher());       
   }
   
   public void testDestroy() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      
      connection.close();
      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000);
      
      rc.destroy();
      
      EasyMock.verify(connection, dispatcher);
      
      EasyMock.reset(connection, dispatcher);
      
      EasyMock.replay(connection, dispatcher);
      
      rc.destroy();
      
      EasyMock.verify(connection, dispatcher);
   }
   
   public void testCreateAndDestroyWithPinger() throws Exception
   {
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      ScheduledExecutorService ex = EasyMock.createStrictMock(ScheduledExecutorService.class);
      
      final long id = 128712;
      EasyMock.expect(dispatcher.generateID()).andReturn(id);
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      
      final long pingPeriod = 1234;
      
      ScheduledFuture future = EasyMock.createStrictMock(ScheduledFuture.class);
      
      EasyMock.expect(ex.scheduleWithFixedDelay(EasyMock.isA(Runnable.class), EasyMock.eq(pingPeriod), EasyMock.eq(pingPeriod),
            EasyMock.eq(TimeUnit.MILLISECONDS))).andReturn(future);
      
      EasyMock.expect(future.cancel(false)).andReturn(true);
      
      dispatcher.unregister(id);
      
      connection.close();
      
      EasyMock.replay(connection, dispatcher, ex);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, pingPeriod, pingPeriod, ex);
      
      rc.destroy();
      
      EasyMock.verify(connection, dispatcher, ex);
      
      EasyMock.reset(connection, dispatcher, ex);
      
      EasyMock.replay(connection, dispatcher, ex);
      
      rc.destroy();
      
      EasyMock.verify(connection, dispatcher, ex);
   }
   
   public void testSendBlockingOK1() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);

      final long id = 128712;
      EasyMock.expect(dispatcher.generateID()).andReturn(id);
      
      class Answer implements IAnswer<Object>
      {
         private volatile PacketHandler handler;

         public PacketHandler getHandler()
         {
            return handler;
         }
         public Object answer() throws Throwable
         {
            handler = (PacketHandler) getCurrentArguments()[0];            
            return null;
         }
      }
      
      final Answer answer = new Answer();
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      EasyMock.expectLastCall().andAnswer(answer);
      
      MessagingBuffer buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andStubReturn(buff);
           
      final Executor queuedEx = Executors.newSingleThreadExecutor();
      
      connection.write(buff);
      EasyMock.expectLastCall().andStubAnswer(
            new IAnswer<Object>()
            {
               public Object answer() throws Throwable
               {
                  //Send pong back on different thread
                  queuedEx.execute(new Runnable()
                  {
                     public void run()
                     {
                        Packet returned = new PacketImpl(PacketImpl.NULL);     
                        answer.getHandler().handle(1243, returned);
                     }
                  });
                  
                  return null;
               }
            });
      
      dispatcher.unregister(id);

      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000L);
            
      class Listener implements FailureListener
      {
         volatile MessagingException me;
         public void connectionFailed(MessagingException me)
         {
            this.me = me;
         }
      }
      
      Listener listener = new Listener();
      
      rc.addFailureListener(listener);
      
      Packet request = new PacketImpl(PacketImpl.CREATECONNECTION);
      
      Packet response = rc.sendBlocking(request);
      
      assertNotNull(response);
      
      assertEquals(PacketImpl.NULL, response.getType());
                  
      EasyMock.verify(connection, dispatcher);
                  
      assertNull(listener.me);
      
   }
   
   public void testSendBlockingOK2() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);

      final long id = 128712;
      EasyMock.expect(dispatcher.generateID()).andReturn(id);
      
      class Answer implements IAnswer<Object>
      {
         private volatile PacketHandler handler;

         public PacketHandler getHandler()
         {
            return handler;
         }
         public Object answer() throws Throwable
         {
            handler = (PacketHandler) getCurrentArguments()[0];            
            return null;
         }
      }
      
      final Answer answer = new Answer();
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      EasyMock.expectLastCall().andAnswer(answer);
      
      MessagingBuffer buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andStubReturn(buff);
           
      final Executor queuedEx = Executors.newSingleThreadExecutor();
      
      Packet packet = EasyMock.createStrictMock(Packet.class);
      
      final long targetID = 139848;
      final long executorID = 3747;
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);
      
      connection.write(buff);
      EasyMock.expectLastCall().andStubAnswer(
            new IAnswer<Object>()
            {
               public Object answer() throws Throwable
               {
                  //Send pong back on different thread
                  queuedEx.execute(new Runnable()
                  {
                     public void run()
                     {
                        Packet returned = new PacketImpl(PacketImpl.NULL);     
                        answer.getHandler().handle(1243, returned);
                     }
                  });
                  
                  return null;
               }
            });
      
      dispatcher.unregister(id);

      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000L);
            
      class Listener implements FailureListener
      {
         volatile MessagingException me;
         public void connectionFailed(MessagingException me)
         {
            this.me = me;
         }
      }
      
      Listener listener = new Listener();
      
      rc.addFailureListener(listener);
      
      Packet request = new PacketImpl(PacketImpl.CREATECONNECTION);
      
      Packet response = rc.sendBlocking(targetID, executorID, request);
      
      assertNotNull(response);
      
      assertEquals(PacketImpl.NULL, response.getType());
                  
      EasyMock.verify(connection, dispatcher);
                  
      assertNull(listener.me);
      
   }
   
   
}
