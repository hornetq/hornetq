/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static java.util.UUID.randomUUID;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class PacketDispatcherTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   PacketDispatcher dispatcher;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      dispatcher = new PacketDispatcher();
   }

   @Override
   protected void tearDown() throws Exception
   {
      dispatcher = null;
   }

   public void testUnregisterAnUnregisteredHandlerReturnsNull() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      String id = randomUUID().toString();
      
      replay(handler);
      
      dispatcher.unregister(id);
      assertNull(dispatcher.getHandler(id));
      
      verify(handler);   
   }
   
   public void testRegisterAndUnregisterValidHandler() throws Exception
   {
      PacketHandler handler = createMock(PacketHandler.class);
      String id = randomUUID().toString();
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
      PacketSender sender = createMock(PacketSender.class);
      
      TextPacket packet = new TextPacket("testDispatch");
      
      String id = randomUUID().toString();
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
