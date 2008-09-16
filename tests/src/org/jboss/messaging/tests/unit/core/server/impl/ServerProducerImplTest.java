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

import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerProducerImplTest extends UnitTestCase
{
   public void testDummy()
   {      
   }
//   private ServerSession session;
//   private CommandManager cm;
//   private FlowController flowController;
//   private PacketDispatcher dispatcher;
//
//   public void testGetId() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      assertEquals(999, producer.getID());
//   }
//
//   public void testSetAndGetWaiting() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      producer.setWaiting(false);
//      assertFalse(producer.isWaiting());
//      producer.setWaiting(true);
//      assertTrue(producer.isWaiting());
//   }
//
//   public void testClose() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      session.removeProducer(producer);
//      replay(session, cm, flowController, dispatcher);
//      producer.close();
//      verify(session, cm, flowController, dispatcher);
//   }
//
//   public void testRequestAndSendCreditsWaiting() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      replay(session, cm, flowController, dispatcher);
//      producer.setWaiting(true);
//      producer.requestAndSendCredits();
//      verify(session, cm, flowController, dispatcher);
//   }
//
//   public void testRequestAndSendCreditsNotWaiting() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      flowController.requestAndSendCredits(producer, 0);
//      replay(session, cm, flowController, dispatcher);
//      producer.setWaiting(false);
//      producer.requestAndSendCredits();
//      verify(session, cm, flowController, dispatcher);
//   }
//
//   public void testSendCreditsWaiting() throws Exception
//   {
//      ServerProducerImpl producer = create(999);
//      cm.sendCommandOneway(EasyMock.eq(1l), (Packet) anyObject());
//      expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            assertEquals(SessionProducerFlowCreditMessage.class, getCurrentArguments()[1].getClass());
//            SessionProducerFlowCreditMessage m = (SessionProducerFlowCreditMessage) getCurrentArguments()[1];
//            assertEquals(m.getTokens(), 12345);
//            return null;
//         }
//      });
//      replay(session, cm, flowController, dispatcher);
//      producer.sendCredits(12345);
//      verify(session, cm, flowController, dispatcher);
//   }
//
//   public void testSend() throws Exception
//   {
//      ServerMessage message = createStrictMock(ServerMessage.class);
//      ServerProducerImpl producer = create(999);
//      expect(message.getEncodeSize()).andReturn(99);
//      session.send((ServerMessage) anyObject());
//      replay(session, cm, flowController, dispatcher, message);
//      producer.send(message);
//      verify(session, cm, flowController, dispatcher, message);
//   }
//
//   public void testSendAndRequestCredits() throws Exception
//   {
//      ServerMessage message = createStrictMock(ServerMessage.class);
//      ServerProducerImpl producer = create(999);
//      expect(message.getEncodeSize()).andReturn(101);
//      flowController.requestAndSendCredits(producer, 101);
//      session.send((ServerMessage) anyObject());
//      replay(session, cm, flowController, dispatcher, message);
//      producer.send(message);
//      verify(session, cm, flowController, dispatcher, message);
//   }
//
//   private ServerProducerImpl create(long id) throws Exception
//   {
//      session = createStrictMock(ServerSession.class);
//      cm = createStrictMock(CommandManager.class);
//      flowController = createStrictMock(FlowController.class);
//      dispatcher = createStrictMock(PacketDispatcher.class);
//      expect(dispatcher.generateID()).andReturn(id);
//      replay(dispatcher);
//      ServerProducerImpl producer = new ServerProducerImpl(session, 1, new SimpleString("testQ"),
//              flowController, 100,
//              dispatcher, cm);
//      verify(dispatcher);
//      reset(dispatcher);
//      return producer;
//   }
}
