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

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerSessionPacketHandler;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerSessionPacketHandlerTest extends UnitTestCase
{
   private ServerSession session;
   private ServerSessionPacketHandler handler;
   private RemotingConnection rc;
   private SimpleString queueName;
   private SimpleString filterString;

   protected void setUp() throws Exception
   {
      session = createStrictMock(ServerSession.class);
      rc = createStrictMock(RemotingConnection.class);
      handler = new ServerSessionPacketHandler(session, rc);
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
      rc.sendOneWay(resp);     
      replay(session, rc);
      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(1, queueName, filterString, true, true, 10, 100);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testCreateQueue() throws Exception
   {
      session.createQueue(queueName, queueName, filterString, true, true);
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL)); 
      replay(session, rc);
      SessionCreateQueueMessage request = new SessionCreateQueueMessage(queueName, queueName, filterString, true, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testDeleteQueue() throws Exception
   {
      session.deleteQueue(queueName);
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL)); 
      replay(session, rc);
      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(queueName);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testQueueQuery() throws Exception
   {
      SessionQueueQueryResponseMessage resp = EasyMock.createStrictMock(SessionQueueQueryResponseMessage.class);
      expect(session.executeQueueQuery(queueName)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testBindingQuery() throws Exception
   {
      SessionBindingQueryResponseMessage resp = EasyMock.createStrictMock(SessionBindingQueryResponseMessage.class);
      expect(session.executeBindingQuery(queueName)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionBindingQueryMessage request = new SessionBindingQueryMessage(queueName);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testCreateBrowser() throws Exception
   {
      SessionCreateBrowserResponseMessage resp = EasyMock.createStrictMock(SessionCreateBrowserResponseMessage.class);
      expect(session.createBrowser(queueName, filterString)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, filterString);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testCreateProducer() throws Exception
   {
      SessionCreateProducerResponseMessage resp = EasyMock.createStrictMock(SessionCreateProducerResponseMessage.class);
      expect(session.createProducer(4, queueName, 33, 44)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionCreateProducerMessage request = new SessionCreateProducerMessage(4, queueName, 33, 44);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testClose() throws Exception
   {
      session.close();
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL));
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.CLOSE);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testSessionAck() throws Exception
   {
      session.acknowledge(44, true);
      replay(session, rc);
      SessionAcknowledgeMessage request = new SessionAcknowledgeMessage(44, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testCommit() throws Exception
   {
      session.commit();
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL));
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_COMMIT);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testRollback() throws Exception
   {
      session.rollback();
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL));
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_ROLLBACK);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testCancel() throws Exception
   {
      session.cancel(55, true);     
      replay(session, rc);
      SessionCancelMessage request = new SessionCancelMessage(55, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaCommit() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XACommit(true, xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXACommitMessage request = new SessionXACommitMessage(xid, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaEnd() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAEnd(xid, true)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAEndMessage request = new SessionXAEndMessage(xid, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaForget() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAForget(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAForgetMessage request = new SessionXAForgetMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaJoin() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAJoin(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAJoinMessage request = new SessionXAJoinMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaResume() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAResume(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAResumeMessage request = new SessionXAResumeMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaRollback() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XARollback(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXARollbackMessage request = new SessionXARollbackMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaStart() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAStart(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAStartMessage request = new SessionXAStartMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaSuspend() throws Exception
   {
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XASuspend()).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaPrepare() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
      expect(session.XAPrepare(xid)).andReturn(resp);
      rc.sendOneWay(resp);
      replay(session, rc);
      SessionXAPrepareMessage request = new SessionXAPrepareMessage(xid);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaInDoubt() throws Exception
   {
      Xid xid = createStrictMock(Xid.class);
      Xid xid2 = createStrictMock(Xid.class);
      List<Xid> xids = new ArrayList<Xid>();
      xids.add(xid);
      xids.add(xid2);
      expect(session.getInDoubtXids()).andReturn(xids);
      rc.sendOneWay(new SessionXAGetInDoubtXidsResponseMessage(xids));
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaGetTimeout() throws Exception
   {
      final int timeout = 2000;
      expect(session.getXATimeout()).andReturn(timeout);
      rc.sendOneWay(new SessionXAGetTimeoutResponseMessage(timeout));
      replay(session, rc);
      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testXaSetTimeout() throws Exception
   {
      expect(session.setXATimeout(5000)).andReturn(true);
      rc.sendOneWay(new SessionXASetTimeoutResponseMessage(true));
      replay(session, rc);
      SessionXASetTimeoutMessage request = new SessionXASetTimeoutMessage(5000);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testAddDestination() throws Exception
   {
      session.addDestination(queueName, true);
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL));
      replay(session, rc);
      SessionAddDestinationMessage request = new SessionAddDestinationMessage(queueName, true);      
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testRemoveDestination() throws Exception
   {
      session.removeDestination(queueName, true);
      rc.sendOneWay(new PacketImpl(PacketImpl.NULL));
      replay(session, rc);
      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(queueName, true);
      handler.handle(123123, request);
      verify(session, rc);
   }

   public void testUnsupportedPacket() throws Exception
   {
      Packet packet = EasyMock.createStrictMock(Packet.class);
      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
      final long responseTargetID = 1212;
      EasyMock.expect(packet.getResponseTargetID()).andReturn(responseTargetID);
      rc.sendOneWay(EasyMock.isA(PacketImpl.class));
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            MessagingExceptionMessage me = (MessagingExceptionMessage)EasyMock.getCurrentArguments()[0];
            assertEquals(MessagingException.UNSUPPORTED_PACKET, me.getException().getCode());
            return null;
         }
      });
      replay(session, rc, packet);
      handler.handle(1212, packet);     
      verify(session, rc, packet);
   }
}
