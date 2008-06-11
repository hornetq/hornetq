/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
