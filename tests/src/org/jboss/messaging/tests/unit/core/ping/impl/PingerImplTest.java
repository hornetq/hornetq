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

package org.jboss.messaging.tests.unit.core.ping.impl;

import junit.framework.TestCase;
import static org.easymock.EasyMock.*;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.ping.impl.PingerImpl;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class PingerImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClose() throws Exception
   {
      long timeout = 500;
 
      RemotingSession session = createMock(RemotingSession.class);
      
      ResponseHandler pongHandler = createMock(ResponseHandler.class);
      long handlerID = randomLong();
      expect(pongHandler.getID()).andReturn(handlerID);
            
      PacketDispatcher dispatcher = createMock(PacketDispatcher.class);
      dispatcher.register(pongHandler);
      expectLastCall().once();
      dispatcher.unregister(handlerID);
      
      CleanUpNotifier failureNotifier = createMock(CleanUpNotifier.class);
      
      replay(dispatcher, session, pongHandler, failureNotifier);

      Pinger pinger = new PingerImpl(dispatcher, session, timeout, pongHandler , failureNotifier);
      pinger.close();
      
      verify(dispatcher, session, pongHandler, failureNotifier);
   }
   
   public void testPingSuccess() throws Exception
   {
      long timeout = 500;
      
      long sessionID = randomLong();
      RemotingSession session = createMock(RemotingSession.class);
      expect(session.getID()).andStubReturn(sessionID);
      
      Ping ping = new Ping(sessionID);
      session.write(ping);
      expectLastCall().once();
      
      Pong pong = new Pong(sessionID, false);      
      ResponseHandler pongHandler = createMock(ResponseHandler.class);
      long handlerID = randomLong();
      expect(pongHandler.getID()).andReturn(handlerID);
      pongHandler.reset();
      expectLastCall().once();
      expect(pongHandler.waitForResponse(timeout)).andReturn(pong);
      
      PacketDispatcher dispatcher = createMock(PacketDispatcher.class);
      dispatcher.register(pongHandler);
      expectLastCall().once();

      CleanUpNotifier failureNotifier = createMock(CleanUpNotifier.class);
      
      replay(dispatcher, session, pongHandler, failureNotifier);
      
      Pinger pinger = new PingerImpl(dispatcher, session, timeout, pongHandler , failureNotifier);
      pinger.run();
      
      verify(dispatcher, session, pongHandler, failureNotifier);
   }
   
   public void testPingFailureWithPongFailed() throws Exception
   {
      long timeout = 500;
      
      long sessionID = randomLong();
      RemotingSession session = createMock(RemotingSession.class);
      expect(session.getID()).andStubReturn(sessionID);
      
      Ping ping = new Ping(sessionID);
      session.write(ping);
      expectLastCall().once();
      
      Pong pong = new Pong(sessionID, true);      
      ResponseHandler pongHandler = createMock(ResponseHandler.class);
      long handlerID = randomLong();
      expect(pongHandler.getID()).andReturn(handlerID);
      pongHandler.reset();
      expectLastCall().once();
      pongHandler.setFailed();
      expectLastCall().once();
      expect(pongHandler.waitForResponse(timeout)).andReturn(pong);
      
      PacketDispatcher dispatcher = createMock(PacketDispatcher.class);
      dispatcher.register(pongHandler);
      expectLastCall().once();

      CleanUpNotifier failureNotifier = createMock(CleanUpNotifier.class);
      failureNotifier.fireCleanup(anyLong(), (MessagingException) anyObject());
      expectLastCall().once();
      
      replay(dispatcher, session, pongHandler, failureNotifier);
      
      Pinger pinger = new PingerImpl(dispatcher, session, timeout, pongHandler , failureNotifier);
      pinger.run();
      
      verify(dispatcher, session, pongHandler, failureNotifier);
   }

   public void testWritePingFailure() throws Exception
   {
      long timeout = 500;
      
      long sessionID = randomLong();
      RemotingSession session = createMock(RemotingSession.class);
      expect(session.getID()).andStubReturn(sessionID);
      
      session.write((Packet) anyObject());
      expectLastCall().andThrow(new Exception());
      
      ResponseHandler pongHandler = createMock(ResponseHandler.class);
      long handlerID = randomLong();
      expect(pongHandler.getID()).andReturn(handlerID);
      pongHandler.reset();
      expectLastCall().once();
      
      PacketDispatcher dispatcher = createMock(PacketDispatcher.class);
      dispatcher.register(pongHandler);
      expectLastCall().once();

      CleanUpNotifier failureNotifier = createMock(CleanUpNotifier.class);
      failureNotifier.fireCleanup(anyLong(), (MessagingException) anyObject());
      expectLastCall().once();
      
      replay(dispatcher, session, pongHandler, failureNotifier);
      
      Pinger pinger = new PingerImpl(dispatcher, session, timeout, pongHandler , failureNotifier);
      pinger.run();
      
      verify(dispatcher, session, pongHandler, failureNotifier);
   }
   
   public void testPingFailure() throws Exception
   {
      long timeout = 500;
      
      long sessionID = randomLong();
      RemotingSession session = createMock(RemotingSession.class);
      expect(session.getID()).andStubReturn(sessionID);
      
      Ping ping = new Ping(sessionID);
      session.write(ping);
      expectLastCall().once();
      
      ResponseHandler pongHandler = createMock(ResponseHandler.class);
      long handlerID = randomLong();
      expect(pongHandler.getID()).andReturn(handlerID);
      pongHandler.reset();
      expectLastCall().once();
      expect(pongHandler.waitForResponse(timeout)).andReturn(null);
      
      PacketDispatcher dispatcher = createMock(PacketDispatcher.class);
      dispatcher.register(pongHandler);
      expectLastCall().once();

      CleanUpNotifier failureNotifier = createMock(CleanUpNotifier.class);
      failureNotifier.fireCleanup(anyLong(), (MessagingException) anyObject());
      expectLastCall().once();
      
      replay(dispatcher, session, pongHandler, failureNotifier);
      
      Pinger pinger = new PingerImpl(dispatcher, session, timeout, pongHandler , failureNotifier);
      pinger.run();
      
      verify(dispatcher, session, pongHandler, failureNotifier);
   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
