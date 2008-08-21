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
package org.jboss.messaging.tests.unit.core.server.impl;

import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.CommandManager;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerBrowserImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="ataylor@tim.fox@jboss.com">Tim Fox</a>
 */
public class ServerBrowserImplTest extends UnitTestCase
{
   public void testConstructor() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      EasyMock.verify(session, destination, dispatcher, cm);
      assertEquals(999l, browser.getID());
   }

   public void testConstructorWithValidFilter() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = "myproperty='this'";
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      EasyMock.verify(session, destination, dispatcher, cm);
      assertEquals(999l, browser.getID());
   }

   public void testConstructorWithInValidFilter() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = "this is a rubbish filter";
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = null;
      try
      {
         browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.INVALID_FILTER_EXPRESSION);
      }
      EasyMock.verify(session, destination, dispatcher, cm);
   }

   public void testClose() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      EasyMock.reset(session);
      session.removeBrowser(browser);
      EasyMock.replay(session);
      browser.close();
      EasyMock.verify(session, destination, dispatcher, cm);
      assertEquals(999l, browser.getID());
   }

   public void testNextMessage() throws Exception
   {
      List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      for (MessageReference ref : refs)
      {
         assertTrue(browser.hasNextMessage());
         assertEquals(browser.nextMessage(), ref.getMessage());
      }
      EasyMock.verify(session, destination, dispatcher, cm);
   }

   public void testReset() throws Exception
   {
      List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andStubReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      for (MessageReference ref : refs)
      {
         assertTrue(browser.hasNextMessage());
         assertEquals(browser.nextMessage(), ref.getMessage());
      }
      browser.reset();
      for (MessageReference ref : refs)
      {
         assertTrue(browser.hasNextMessage());
         assertEquals(browser.nextMessage(), ref.getMessage());
      }
      EasyMock.verify(session, destination, dispatcher, cm);
   }

   public void testNextBlock() throws Exception
   {
      List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      Message[] messages = browser.nextMessageBlock(2);
      Message[] messages2 = browser.nextMessageBlock(3);
      assertEquals(messages.length, 2);
      assertEquals(messages2.length, 3);
      assertEquals(messages[0], refs.get(0).getMessage());
      assertEquals(messages[1], refs.get(1).getMessage());
      assertEquals(messages2[0], refs.get(2).getMessage());
      assertEquals(messages2[1], refs.get(3).getMessage());
      assertEquals(messages2[2], refs.get(4).getMessage());
      EasyMock.verify(session, destination, dispatcher, cm);
   }


   public void testNextBlockWithOneThrowsException() throws Exception
   {
      List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.replay(session, destination, dispatcher, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      Message[] messages = browser.nextMessageBlock(2);
      try
      {
         browser.nextMessageBlock(1);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
      assertEquals(messages.length, 2);
      assertEquals(messages[0], refs.get(0).getMessage());
      assertEquals(messages[1], refs.get(1).getMessage());
      EasyMock.verify(session, destination, dispatcher, cm);
   }

   public void testCloseDoHandle() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.CLOSE);
      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(1l);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject());
      cm.packetProcessed(packet);
      EasyMock.replay(session, destination, dispatcher, packet, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      EasyMock.reset(session);
      session.removeBrowser(browser);
      EasyMock.replay(session);
      PacketHandler handler = browser.newHandler();
      handler.handle(132, packet);
      EasyMock.verify(session, destination, dispatcher, cm);
      assertEquals(999l, browser.getID());
   }

   public void testHasNextMessageDoHandle() throws Exception
   {
      final List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andStubReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE);
      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(1l);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject());      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(1, targetID);
            SessionBrowserHasNextMessageResponseMessage resp =
               (SessionBrowserHasNextMessageResponseMessage) EasyMock.getCurrentArguments()[1];
            assertEquals(resp.hasNext(), true);
            return null;
         }         
      });
      cm.packetProcessed(packet);
      EasyMock.replay(session, destination, dispatcher, packet, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      PacketHandler handler = browser.newHandler();
      handler.handle(1245, packet);
      EasyMock.verify(session, destination, dispatcher, packet, cm);
      assertEquals(999l, handler.getID());
   }

   public void testNextMessageDoHandle() throws Exception
   {
      final List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andStubReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(1l);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject());      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(1, targetID);
            ReceiveMessage resp = (ReceiveMessage) EasyMock.getCurrentArguments()[1];
            assertEquals(resp.getServerMessage(), refs.get(0).getMessage());
            return null;
         }
      });
      cm.packetProcessed(packet);
      EasyMock.replay(session, destination, dispatcher, packet, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      PacketHandler handler = browser.newHandler();
      handler.handle(1762162, packet);
      EasyMock.verify(session, destination, dispatcher, packet, cm);
      assertEquals(999l, browser.getID());
   }

   public void testResetDoHandle() throws Exception
   {
      final List<MessageReference> refs = setupList();
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(destination.list(null)).andStubReturn(refs);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(1l);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.SESS_BROWSER_RESET);
      EasyMock.expect(packet.getType()).andReturn(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject());      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(1, targetID);
            ReceiveMessage resp = (ReceiveMessage) EasyMock.getCurrentArguments()[1];
            assertEquals(resp.getServerMessage(), refs.get(0).getMessage());
            return null;
         }
      });
      cm.packetProcessed(packet);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject()) ;
      cm.packetProcessed(packet);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject()) ;
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(1, targetID);
            ReceiveMessage resp = (ReceiveMessage) EasyMock.getCurrentArguments()[1];
            assertEquals(resp.getServerMessage(), refs.get(0).getMessage());
            return null;
         }
      });
      cm.packetProcessed(packet);
      EasyMock.replay(session, destination, dispatcher, packet, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      PacketHandler handler = browser.newHandler();
      handler.handle(1234, packet);
      handler.handle(1234, packet);
      handler.handle(1234, packet);
      EasyMock.verify(session, destination, dispatcher, packet, cm);
      assertEquals(999l, browser.getID());
   }

   public void testUnhandledDoHandle() throws Exception
   {
      ServerSession session = EasyMock.createStrictMock(ServerSession.class);
      Queue destination = EasyMock.createStrictMock(Queue.class);
      String messageFilter = null;
      PacketDispatcher dispatcher = EasyMock.createStrictMock(PacketDispatcher.class);
      Packet packet = EasyMock.createStrictMock(Packet.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      EasyMock.expect(dispatcher.generateID()).andReturn(999l);
      EasyMock.expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(1l);
      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) EasyMock.anyObject());      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(1, targetID);
            MessagingExceptionMessage message = (MessagingExceptionMessage) EasyMock.getCurrentArguments()[1];
            assertEquals(message.getException().getCode(), MessagingException.UNSUPPORTED_PACKET);
            return null;
         }
      });
      cm.packetProcessed(packet);
      EasyMock.replay(session, destination, dispatcher, packet, cm);
      ServerBrowserImpl browser = new ServerBrowserImpl(session, destination, messageFilter, dispatcher, cm);
      PacketHandler handler = browser.newHandler();

      handler.handle(1245, packet);
      EasyMock.verify(session, destination, dispatcher, packet, cm);
      assertEquals(999l, browser.getID());
   }

   private List<MessageReference> setupList()
   {
      ServerMessage serverMessage1 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage serverMessage3 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage serverMessage4 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage serverMessage5 = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference reference1 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference3 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference4 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference5 = EasyMock.createStrictMock(MessageReference.class);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      refs.add(reference1);
      refs.add(reference2);
      refs.add(reference3);
      refs.add(reference4);
      refs.add(reference5);
      EasyMock.expect(reference1.getMessage()).andStubReturn(serverMessage1);
      EasyMock.expect(reference2.getMessage()).andStubReturn(serverMessage2);
      EasyMock.expect(reference3.getMessage()).andStubReturn(serverMessage3);
      EasyMock.expect(reference4.getMessage()).andStubReturn(serverMessage4);
      EasyMock.expect(reference5.getMessage()).andStubReturn(serverMessage5);
      EasyMock.replay(reference1, reference2, reference3, reference4, reference5);
      return refs;
   }
}
