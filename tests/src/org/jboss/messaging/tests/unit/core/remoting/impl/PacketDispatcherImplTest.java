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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcherImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private PacketDispatcherImpl dispatcher;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      dispatcher = new PacketDispatcherImpl(null);
   }

   @Override
   protected void tearDown() throws Exception
   {
      dispatcher = null;
   }

   public void testGenerateId()
   {
      for (int i = 1; i < 1000; i++)
      {
         assertEquals(dispatcher.generateID(), i);
      }            
   }
   
   public void testDispatch() throws Exception
   {
      PacketHandler handler1 = createMock(PacketHandler.class);
      PacketHandler handler2 = createMock(PacketHandler.class);
      Packet packet1 = createMock(Packet.class);
      Packet packet2 = createMock(Packet.class);
      long handlerID1 = randomLong();
      expect(handler1.getID()).andStubReturn(handlerID1);
      long handlerID2 = randomLong();
      expect(handler2.getID()).andStubReturn(handlerID2);
      long connectionID = 121212;
      expect(packet1.getTargetID()).andReturn(handlerID1);
      expect(packet2.getTargetID()).andReturn(handlerID2);
      handler1.handle(connectionID, packet1);
      handler2.handle(connectionID, packet2);
      replay(handler1, packet1, handler2, packet2);
      dispatcher = new PacketDispatcherImpl(null);
      dispatcher.register(handler1);
      dispatcher.register(handler2);
      dispatcher.dispatch(connectionID, packet1);
      dispatcher.dispatch(connectionID, packet2);
      verify(handler1, packet1, handler2, packet2);
      
      reset(handler1, packet1, handler2, packet2);
      expect(handler1.getID()).andStubReturn(handlerID1);
      expect(handler2.getID()).andStubReturn(handlerID2);
      expect(packet1.getTargetID()).andReturn(handlerID1);
      expect(packet2.getTargetID()).andReturn(handlerID2);
      expect(packet1.getType()).andReturn(PacketImpl.CLOSE);
      expect(packet2.getType()).andReturn(PacketImpl.CLOSE);
      replay(handler1, packet1, handler2, packet2);

      dispatcher.unregister(handler1.getID());
      dispatcher.unregister(handler2.getID());
      
      dispatcher.dispatch(connectionID, packet1);
      dispatcher.dispatch(connectionID, packet2);
      verify(handler1, packet1, handler2, packet2);
   }

   public void testInterceptorsCalledReturningTrue() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      Packet packet = createMock(Packet.class);
      Interceptor interceptor1 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      ArrayList<Interceptor> interceptors = new ArrayList<Interceptor>();
      interceptors.add(interceptor1);
      interceptors.add(interceptor2);
      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      long connectionID = 121212;
      handler.handle(connectionID, packet);
      expect(packet.getTargetID()).andReturn(id);
      EasyMock.expect(interceptor1.intercept(packet)).andReturn(true);
      EasyMock.expect(interceptor2.intercept(packet)).andReturn(true);
      replay(handler, packet, interceptor1, interceptor2);
      dispatcher = new PacketDispatcherImpl(interceptors);
      dispatcher.register(handler);
      dispatcher.dispatch(connectionID, packet);
      verify(handler, packet, interceptor1, interceptor2);
   }
   
   public void testInterceptorsCalledOneReturningFalse() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      Packet packet = createMock(Packet.class);
      Interceptor interceptor1 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      ArrayList<Interceptor> interceptors = new ArrayList<Interceptor>();
      interceptors.add(interceptor1);
      interceptors.add(interceptor2);
      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      long connectionID = 121212;
      expect(packet.getTargetID()).andReturn(id);
      EasyMock.expect(interceptor1.intercept(packet)).andReturn(true);
      EasyMock.expect(interceptor2.intercept(packet)).andReturn(false);
      replay(handler, packet, interceptor1, interceptor2);
      dispatcher = new PacketDispatcherImpl(interceptors);
      dispatcher.register(handler);
      dispatcher.dispatch(connectionID, packet);
      verify(handler, packet, interceptor1, interceptor2);
   }

   public void testUnregisterAnUnregisteredHandlerReturnsNull() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      long id = randomLong();

      replay(handler);

      try
      {
         dispatcher.unregister(id);
         fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
      assertNull(dispatcher.getHandler(id));

      verify(handler);
   }

   public void testRegisterAndUnregisterValidHandler() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      long id = randomLong();
      expect(handler.getID()).andReturn(id).anyTimes();

      replay(handler);

      dispatcher.register(handler);

      PacketHandler registeredHandler = dispatcher.getHandler(id);
      assertSame(handler, registeredHandler);

      dispatcher.unregister(id);
      assertNull(dispatcher.getHandler(id));

      verify(handler);
   }

   public void testAddInterceptor() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(interceptor.intercept(packet)).andReturn(true);
      EasyMock.replay(interceptor, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.callInterceptors(packet);
      EasyMock.verify(interceptor, packet);
   }

   public void testAddInterceptors() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(interceptor.intercept(packet)).andReturn(true);
      expect(interceptor2.intercept(packet)).andReturn(true);
      expect(interceptor3.intercept(packet)).andReturn(true);
      EasyMock.replay(interceptor, interceptor2, interceptor3, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.addInterceptor(interceptor2);
      dispatcher.addInterceptor(interceptor3);
      dispatcher.callInterceptors(packet);
      EasyMock.verify(interceptor, interceptor2, interceptor3, packet);
   }

   public void testAddAndRemoveInterceptor() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(interceptor.intercept(packet)).andReturn(true);
      EasyMock.replay(interceptor, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.callInterceptors(packet);
      dispatcher.removeInterceptor(interceptor);
      dispatcher.callInterceptors(packet);
      EasyMock.verify(interceptor, packet);
   }

   public void testAddAndRemoveInterceptors() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(interceptor.intercept(packet)).andReturn(true);
      expect(interceptor2.intercept(packet)).andReturn(true);
      expect(interceptor3.intercept(packet)).andReturn(true);
      expect(interceptor.intercept(packet)).andReturn(true);
      expect(interceptor3.intercept(packet)).andReturn(true);
      EasyMock.replay(interceptor, interceptor2, interceptor3, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.addInterceptor(interceptor2);
      dispatcher.addInterceptor(interceptor3);
      dispatcher.callInterceptors(packet);
      dispatcher.removeInterceptor(interceptor2);
      dispatcher.callInterceptors(packet);
      EasyMock.verify(interceptor, interceptor2, interceptor3, packet);
   }

   public void testInterceptorThrowingException() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      interceptor.intercept(packet);
      EasyMock.expectLastCall().andThrow(new RuntimeException());
      EasyMock.replay(interceptor, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.callInterceptors(packet);
      EasyMock.verify(interceptor, packet);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
