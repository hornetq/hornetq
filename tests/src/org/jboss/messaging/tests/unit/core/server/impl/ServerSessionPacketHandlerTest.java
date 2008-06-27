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

import static org.easymock.EasyMock.*;
import org.easymock.classextension.EasyMock;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.*;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerSessionPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerSessionPacketHandlerTest extends UnitTestCase
{
   private ServerSession session;
   private ServerSessionPacketHandler handler;
   private PacketReturner sender;
   private SimpleString queueName;
   private SimpleString filterString;

   protected void setUp() throws Exception
   {
      session = createStrictMock(ServerSession.class);
      sender = createStrictMock(PacketReturner.class);
      handler = new ServerSessionPacketHandler(session);
      queueName = new SimpleString("qname");
      filterString = new SimpleString("test = 'foo'");
   }

   protected void tearDown() throws Exception
   {
      session = null;
      handler = null;
   }

   public void testGetId()
   {
      expect(handler.getID()).andReturn(999l);
      replay(session);
      assertEquals(handler.getID(), 999l);
      verify(session);
   }

   public void testCreateConsumer() throws Exception
   {
      SessionCreateConsumerResponseMessage resp = EasyMock.createStrictMock(SessionCreateConsumerResponseMessage.class);
      expect(session.createConsumer(1, queueName, filterString, true, true, 10, 100)).andReturn(resp);
      replay(session, sender);
      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(1, queueName, filterString, true, true, 10, 100);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testCreateQueue() throws Exception
   {
      session.createQueue(queueName, queueName, filterString, true, true);
      replay(session, sender);
      SessionCreateQueueMessage request = new SessionCreateQueueMessage(queueName, queueName, filterString, true, true);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testDeleteQueue() throws Exception
   {
      session.deleteQueue(queueName);
      replay(session, sender);
      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(queueName);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testQueueQuery() throws Exception
   {
      SessionQueueQueryResponseMessage resp = EasyMock.createStrictMock(SessionQueueQueryResponseMessage.class);
      expect(session.executeQueueQuery(queueName)).andReturn(resp);
      replay(session, sender);
      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testBindingQuery() throws Exception
   {
      SessionBindingQueryResponseMessage resp = EasyMock.createStrictMock(SessionBindingQueryResponseMessage.class);
      expect(session.executeBindingQuery(queueName)).andReturn(resp);
      replay(session, sender);
      SessionBindingQueryMessage request = new SessionBindingQueryMessage(queueName);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testCreateBrowser() throws Exception
   {
      SessionCreateBrowserResponseMessage resp = EasyMock.createStrictMock(SessionCreateBrowserResponseMessage.class);
      expect(session.createBrowser(queueName, filterString)).andReturn(resp);
      replay(session, sender);
      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, filterString);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testCreateProducer() throws Exception
   {
      SessionCreateProducerResponseMessage resp = EasyMock.createStrictMock(SessionCreateProducerResponseMessage.class);
      expect(session.createProducer(4, queueName, 33, 44)).andReturn(resp);
      replay(session, sender);
      SessionCreateProducerMessage request = new SessionCreateProducerMessage(4, queueName, 33, 44);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testClose() throws Exception
   {
      session.close();
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.CLOSE);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testSessionAck() throws Exception
   {
      session.acknowledge(44, true);
      replay(session, sender);
      SessionAcknowledgeMessage request = new SessionAcknowledgeMessage(44, true);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testCommit() throws Exception
   {
      session.commit();
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_COMMIT);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testRollback() throws Exception
   {
      session.rollback();
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_ROLLBACK);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testCancel() throws Exception
   {
      session.cancel(55, true);
      replay(session, sender);
      SessionCancelMessage request = new SessionCancelMessage(55, true);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaCommit() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XACommit(true, xid)).andReturn(resp);
      replay(session, sender);
      SessionXACommitMessage request = new SessionXACommitMessage(xid, true);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaEnd() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAEnd(xid, true)).andReturn(resp);
      replay(session, sender);
      SessionXAEndMessage request = new SessionXAEndMessage(xid, true);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaForget() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAForget(xid)).andReturn(resp);
      replay(session, sender);
      SessionXAForgetMessage request = new SessionXAForgetMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaJoin() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAJoin(xid)).andReturn(resp);
      replay(session, sender);
      SessionXAJoinMessage request = new SessionXAJoinMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaResume() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAResume(xid)).andReturn(resp);
      replay(session, sender);
      SessionXAResumeMessage request = new SessionXAResumeMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaRollback() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XARollback(xid)).andReturn(resp);
      replay(session, sender);
      SessionXARollbackMessage request = new SessionXARollbackMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaStart() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAStart(xid)).andReturn(resp);
      replay(session, sender);
      SessionXAStartMessage request = new SessionXAStartMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaSuspend() throws Exception
   {
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XASuspend()).andReturn(resp);
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaPrepare() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAPrepare(xid)).andReturn(resp);
      replay(session, sender);
      SessionXAPrepareMessage request = new SessionXAPrepareMessage(xid);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaInDoubt() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      Xid xid2 = createStrictMock(Xid.class);
      List<Xid> xids = new ArrayList<Xid>();
      xids.add(xid);
      xids.add(xid2);
      expect(session.getInDoubtXids()).andReturn(xids);
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
      SessionXAGetInDoubtXidsResponseMessage resp = (SessionXAGetInDoubtXidsResponseMessage) handler.doHandle(request, sender);
      assertEquals(resp.getXids().size(), 2);
      assertEquals(resp.getXids().get(0), xid);
      assertEquals(resp.getXids().get(1), xid2);
      verify(session, sender);
   }

   public void testXaGetTimeout() throws Exception
   {
      expect(session.getXATimeout()).andReturn(2000);
      replay(session, sender);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
      SessionXAGetTimeoutResponseMessage resp = (SessionXAGetTimeoutResponseMessage) handler.doHandle(request, sender);
      assertEquals(resp.getTimeoutSeconds(), 2000);
      verify(session, sender);
   }

   public void testXaSetTimeout() throws Exception
   {
      expect(session.setXATimeout(5000)).andReturn(true);
      replay(session, sender);
      SessionXASetTimeoutMessage request = new SessionXASetTimeoutMessage(5000);
      SessionXASetTimeoutResponseMessage resp = (SessionXASetTimeoutResponseMessage) handler.doHandle(request, sender);
      assertEquals(resp.isOK(), true);
      verify(session, sender);
   }

   public void testXaAddDestination() throws Exception
   {
      session.addDestination(queueName, true);
      replay(session, sender);
      SessionAddDestinationMessage request = new SessionAddDestinationMessage(queueName, true);
      request.setResponseTargetID(12345);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

   public void testXaRemoveDestination() throws Exception
   {
      session.removeDestination(queueName, true);
      replay(session, sender);
      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(queueName, true);
      request.setResponseTargetID(12345);
      handler.doHandle(request, sender);
      verify(session, sender);
   }

    public void testUnsupportedPacket() throws Exception
   {
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      replay(session, sender);
      try
      {
         handler.doHandle(packet, sender);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         MessagingException messagingException = (MessagingException) e;
         assertEquals(messagingException.getCode(), MessagingException.UNSUPPORTED_PACKET);
      }

      verify(session, sender);
   }
}
