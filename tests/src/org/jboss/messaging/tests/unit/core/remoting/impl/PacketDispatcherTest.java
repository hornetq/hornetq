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

import junit.framework.TestCase;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.ArrayList;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcherTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   PacketDispatcherImpl dispatcher;

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

   public void testFiltersCalled() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      PacketReturner sender = createMock(PacketReturner.class);
      Packet packet = createMock(Packet.class);
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      ArrayList<Interceptor> filters = new ArrayList<Interceptor>();
      filters.add(interceptor);
      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      handler.handle(packet, sender);
      expectLastCall().once();
      expect(packet.getTargetID()).andReturn(id);
      interceptor.intercept(packet);
      replay(handler, sender, packet, interceptor);

      dispatcher = new PacketDispatcherImpl(filters);
      dispatcher.register(handler);
      dispatcher.dispatch(packet, sender);

      verify(interceptor);
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

   public void testDispatchPacketWithRegisteredHandler() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      PacketReturner sender = createMock(PacketReturner.class);

      Ping packet = new Ping(randomLong());

      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      handler.handle(packet, sender);
      expectLastCall().once();

      replay(handler, sender);

      dispatcher.register(handler);

      PacketHandler registeredHandler = dispatcher.getHandler(id);
      assertSame(handler, registeredHandler);

      packet.setTargetID(handler.getID());
      dispatcher.dispatch(packet, sender);

      dispatcher.unregister(id);
      assertNull(dispatcher.getHandler(id));

      verify(handler, sender);
   }

   public void testDispatchPacketWithNoIDSetIsNotDispatched() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      PacketReturner sender = createMock(PacketReturner.class);
      Packet packet = createMock(Packet.class);

      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      expect(packet.getTargetID()).andReturn(-1l);
      expectLastCall().once();

      replay(handler, sender, packet);

      dispatcher.register(handler);

      PacketHandler registeredHandler = dispatcher.getHandler(id);
      assertSame(handler, registeredHandler);

      dispatcher.dispatch(packet, sender);

      dispatcher.unregister(id);
      assertNull(dispatcher.getHandler(id));

      verify(handler, sender);
   }

   public void testDispatchPacketWithUnRegisteredHandlerNoException() throws Exception
   {
      PacketReturner sender = createMock(PacketReturner.class);

      Ping packet = new Ping(randomLong());

      long id = randomLong();

      replay(sender);

      packet.setTargetID(id);
      dispatcher.dispatch(packet, sender);

      verify(sender);
   }


   public void testRegistrationListener() throws Exception
   {
      PacketHandlerRegistrationListener listener = createMock(PacketHandlerRegistrationListener.class);
      PacketHandler handler = createMock(PacketHandler.class);

      long id = randomLong();
      expect(handler.getID()).andStubReturn(id);
      listener.handlerRegistered(id);
      expectLastCall().once();
      listener.handlerUnregistered(id);
      expectLastCall().once();

      replay(handler, listener);

      dispatcher.setListener(listener);
      dispatcher.register(handler);
      dispatcher.unregister(id);

      verify(handler, listener);
   }

   public void testAddInterceptor() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      interceptor.intercept(packet);
      EasyMock.replay(interceptor, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.callFilters(packet);
      EasyMock.verify(interceptor, packet);
   }

   public void testAddInterceptors() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
      Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      interceptor.intercept(packet);
      interceptor2.intercept(packet);
      interceptor3.intercept(packet);
      EasyMock.replay(interceptor, interceptor2, interceptor3, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.addInterceptor(interceptor2);
      dispatcher.addInterceptor(interceptor3);
      dispatcher.callFilters(packet);
      EasyMock.verify(interceptor, interceptor2, interceptor3, packet);
   }

   public void testAddAndRemoveInterceptor() throws Exception
   {
      Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      interceptor.intercept(packet);
      EasyMock.replay(interceptor, packet);
      dispatcher.addInterceptor(interceptor);
      dispatcher.callFilters(packet);
      dispatcher.removeInterceptor(interceptor);
      dispatcher.callFilters(packet);
      EasyMock.verify(interceptor, packet);
   }

   public void testAddAndRemoveInterceptors() throws Exception
     {
        Interceptor interceptor = EasyMock.createStrictMock(Interceptor.class);
        Interceptor interceptor2 = EasyMock.createStrictMock(Interceptor.class);
        Interceptor interceptor3 = EasyMock.createStrictMock(Interceptor.class);
        Packet packet = EasyMock.createStrictMock(Packet.class);
        interceptor.intercept(packet);
        interceptor2.intercept(packet);
        interceptor3.intercept(packet);
        interceptor.intercept(packet);
        interceptor3.intercept(packet);
        EasyMock.replay(interceptor, interceptor2, interceptor3, packet);
        dispatcher.addInterceptor(interceptor);
        dispatcher.addInterceptor(interceptor2);
        dispatcher.addInterceptor(interceptor3);
        dispatcher.callFilters(packet);
        dispatcher.removeInterceptor(interceptor2);
        dispatcher.callFilters(packet);
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
      dispatcher.callFilters(packet);
      EasyMock.verify(interceptor, packet);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
