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
package org.jboss.messaging.tests.timing.core.remoting.impl;

import static org.easymock.EasyMock.getCurrentArguments;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
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
   private static final Logger log = Logger.getLogger(RemotingConnectionImplTest.class);

   public void testPingPongOK() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      ScheduledExecutorService ex = new ScheduledThreadPoolExecutor(1);
      
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
           
      final AtomicInteger pongs = new AtomicInteger(0);
      
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
                        Packet pong = new PacketImpl(PacketImpl.PONG);     
                        answer.getHandler().handle(1243, pong);
                        pongs.incrementAndGet();
                     }
                  });
                  
                  return null;
               }
            });

      final long pingPeriod = 100;
      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000L, pingPeriod, ex);
            
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
      
      Thread.sleep((long)(10 * pingPeriod + 1.5 * pingPeriod));
      
      EasyMock.verify(connection, dispatcher);
      
      assertTrue(pongs.get() >= 10);
      
      assertNull(listener.me);
      
   }
   
   public void testPingNoPong() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      ScheduledExecutorService ex = new ScheduledThreadPoolExecutor(1);
      
      final long id = 128712;
      EasyMock.expect(dispatcher.generateID()).andReturn(id);
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      
      MessagingBuffer buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andStubReturn(buff);
                 
      connection.write(buff);
      EasyMock.expectLastCall().anyTimes();
      
      dispatcher.unregister(id);
      
      connection.close();

      final long pingPeriod = 100;
      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000L, pingPeriod, ex);
            
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
      
      Thread.sleep((long)(2.5 * pingPeriod));
      
      EasyMock.verify(connection, dispatcher);
            
      assertNotNull(listener.me);
      
   }
   
   
   public void testPingLatePong() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);
      ScheduledExecutorService ex = new ScheduledThreadPoolExecutor(1);
      
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
      
      final long pingPeriod = 100;      
      
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
                        try
                        {
                           Thread.sleep((long)(1.5 * pingPeriod));
                        }
                        catch (Exception e)
                        {                           
                        }
                        Packet pong = new PacketImpl(PacketImpl.PONG);     
                        answer.getHandler().handle(1243, pong);                     
                     }
                  });
                  
                  return null;
               }
            });

      
      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, 1000L, pingPeriod, ex);
            
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
      
      Thread.sleep(3 * pingPeriod);
      
      EasyMock.verify(connection, dispatcher);
      
      assertNull(listener.me);
      
   }
   
   public void testSendBlockingNoResponse() throws Exception
   {     
      Connection connection = EasyMock.createStrictMock(Connection.class);
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Location location = new LocationImpl(TransportType.TCP, "blah", 1234);

      final long id = 128712;
      EasyMock.expect(dispatcher.generateID()).andReturn(id);
      
      dispatcher.register(EasyMock.isA(PacketHandler.class));
      
      MessagingBuffer buff = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      EasyMock.expect(connection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE)).andStubReturn(buff);
             
      connection.write(buff);
      
      connection.close();
          
      dispatcher.unregister(id);

      EasyMock.replay(connection, dispatcher);
      
      final long callTimeout = 100;
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, callTimeout);
            
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
      
      Packet request = new PacketImpl(PacketImpl.CREATESESSION);
      
      long start = System.currentTimeMillis();
      try
      {
         rc.sendBlocking(request, null);
         fail("Should throw exception");
      }
      catch (MessagingException me)
      {
         assertEquals(MessagingException.CONNECTION_TIMEDOUT, me.getCode());
      }
      long end = System.currentTimeMillis();
      
      assertTrue((end - start) >= callTimeout && (end - start) <= 1.5 * callTimeout);
                              
      EasyMock.verify(connection, dispatcher);
                  
      assertNotNull(listener.me);  
      
      assertEquals(MessagingException.CONNECTION_TIMEDOUT, listener.me.getCode());
   }
   
   
   public void testSendBlockingLateResponse() throws Exception
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
      
      final long callTimeout = 100;
      
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
                        try
                        {
                           Thread.sleep((long)(1.5 * callTimeout));
                        }
                        catch (Exception e)
                        {                           
                        }
                        Packet returned = new PacketImpl(PacketImpl.NULL);     
                        answer.getHandler().handle(1243, returned);
                     }
                  });
                  
                  return null;
               }
            });
      
      connection.close();
      
      dispatcher.unregister(id);

      EasyMock.replay(connection, dispatcher);
      
      RemotingConnection rc = new RemotingConnectionImpl(connection, dispatcher, location, callTimeout);
            
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
      
      Packet request = new PacketImpl(PacketImpl.CREATESESSION);
      
      try
      {
         rc.sendBlocking(request, null);
         fail("Should throw exception");      
      }
      catch (MessagingException me)
      {
         assertEquals(MessagingException.CONNECTION_TIMEDOUT, me.getCode());
      }
                             
      EasyMock.verify(connection, dispatcher);
                  
      assertNotNull(listener.me);
      
      assertEquals(MessagingException.CONNECTION_TIMEDOUT, listener.me.getCode());
      
   }
  
}
