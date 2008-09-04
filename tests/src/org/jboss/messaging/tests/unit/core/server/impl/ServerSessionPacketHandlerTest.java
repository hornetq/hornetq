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
public class ServerSessionPacketHandlerTest extends UnitTestCase
{
   public void testDummy()
   {      
   }
//   private ServerSession session;
//   private ServerSessionPacketHandler handler;
//   private CommandManager cm;
//   private SimpleString queueName;
//   private SimpleString filterString;
//
//   protected void setUp() throws Exception
//   {
//      session = createStrictMock(ServerSession.class);
//      cm = createStrictMock(CommandManager.class);
//      handler = new ServerSessionPacketHandler(session, cm);
//      queueName = new SimpleString("qname");
//      filterString = new SimpleString("test = 'foo'");
//   }
//
//   protected void tearDown() throws Exception
//   {
//      session = null;
//      handler = null;
//   }
//
//   public void testGetId()
//   {
//      expect(handler.getID()).andReturn(999l);
//      replay(session);
//      assertEquals(handler.getID(), 999l);
//      verify(session);
//   }
//
//   public void testCreateConsumer() throws Exception
//   {
//      SessionCreateConsumerResponseMessage resp = EasyMock.createStrictMock(SessionCreateConsumerResponseMessage.class);
//      expect(session.createConsumer(1, queueName, filterString, 10, 100)).andReturn(resp);
//      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(1, queueName, filterString, 10, 100);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      cm.sendCommandOneway(responseTargetID, resp);  
//      cm.packetProcessed(request);
//      replay(session, cm);
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testCreateQueue() throws Exception
//   {
//      SessionCreateQueueMessage request = new SessionCreateQueueMessage(queueName, queueName, filterString, true, true);
//      session.createQueue(queueName, queueName, filterString, true, true);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL)); 
//      cm.packetProcessed(request);
//      replay(session, cm);
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testDeleteQueue() throws Exception
//   {
//      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(queueName);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      session.deleteQueue(queueName);
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL)); 
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testQueueQuery() throws Exception
//   {
//      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
//      SessionQueueQueryResponseMessage resp = EasyMock.createStrictMock(SessionQueueQueryResponseMessage.class);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      expect(session.executeQueueQuery(queueName)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testBindingQuery() throws Exception
//   {
//      SessionBindingQueryMessage request = new SessionBindingQueryMessage(queueName);
//      SessionBindingQueryResponseMessage resp = EasyMock.createStrictMock(SessionBindingQueryResponseMessage.class);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      expect(session.executeBindingQuery(queueName)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testCreateBrowser() throws Exception
//   {
//      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, filterString);
//      SessionCreateBrowserResponseMessage resp = EasyMock.createStrictMock(SessionCreateBrowserResponseMessage.class);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      expect(session.createBrowser(queueName, filterString)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testCreateProducer() throws Exception
//   {
//      SessionCreateProducerMessage request = new SessionCreateProducerMessage(4, queueName, 33, 44);
//      SessionCreateProducerResponseMessage resp = EasyMock.createStrictMock(SessionCreateProducerResponseMessage.class);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      expect(session.createProducer(4, queueName, 33, 44)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testClose() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.CLOSE);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      session.close();
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testSessionAck() throws Exception
//   {
//      SessionAcknowledgeMessage request = new SessionAcknowledgeMessage(44, true);     
//      session.acknowledge(44, true);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testCommit() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.SESS_COMMIT);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      session.commit();
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testRollback() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.SESS_ROLLBACK);
//      long responseTargetID = 1212;
//      request.setResponseTargetID(responseTargetID);
//      session.rollback();
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      cm.packetProcessed(request);
//      replay(session, cm);    
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testCancel() throws Exception
//   {
//      SessionCancelMessage request = new SessionCancelMessage(55, true);
//      session.cancel(55, true);     
//      cm.packetProcessed(request);
//      replay(session, cm);
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaCommit() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXACommitMessage request = new SessionXACommitMessage(xid, true);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);      
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XACommit(true, xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaEnd() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAEndMessage request = new SessionXAEndMessage(xid, true);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID); 
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAEnd(xid, true)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);     
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaForget() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAForgetMessage request = new SessionXAForgetMessage(xid); 
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID); 
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAForget(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaJoin() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAJoinMessage request = new SessionXAJoinMessage(xid);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);       
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAJoin(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaResume() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAResumeMessage request = new SessionXAResumeMessage(xid);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);        
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAResume(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaRollback() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXARollbackMessage request = new SessionXARollbackMessage(xid);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);  
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XARollback(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);     
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaStart() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAStartMessage request = new SessionXAStartMessage(xid);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);  
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAStart(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaSuspend() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XASuspend()).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaPrepare() throws Exception
//   {
//      Xid xid = createStrictMock(Xid.class);
//      SessionXAPrepareMessage request = new SessionXAPrepareMessage(xid);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      SessionXAResponseMessage resp = EasyMock.createStrictMock(SessionXAResponseMessage.class);
//      expect(session.XAPrepare(xid)).andReturn(resp);
//      cm.sendCommandOneway(responseTargetID, resp);
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaInDoubt() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      Xid xid = createStrictMock(Xid.class);
//      Xid xid2 = createStrictMock(Xid.class);
//      List<Xid> xids = new ArrayList<Xid>();
//      xids.add(xid);
//      xids.add(xid2);
//      expect(session.getInDoubtXids()).andReturn(xids);
//      cm.sendCommandOneway(responseTargetID, new SessionXAGetInDoubtXidsResponseMessage(xids));
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaGetTimeout() throws Exception
//   {
//      PacketImpl request = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      final int timeout = 2000;
//      expect(session.getXATimeout()).andReturn(timeout);
//      cm.sendCommandOneway(responseTargetID, new SessionXAGetTimeoutResponseMessage(timeout));
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testXaSetTimeout() throws Exception
//   {
//      SessionXASetTimeoutMessage request = new SessionXASetTimeoutMessage(5000);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      expect(session.setXATimeout(5000)).andReturn(true);
//      cm.sendCommandOneway(responseTargetID, new SessionXASetTimeoutResponseMessage(true));
//      cm.packetProcessed(request);
//      replay(session, cm);     
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testAddDestination() throws Exception
//   {
//      SessionAddDestinationMessage request = new SessionAddDestinationMessage(queueName, true, true); 
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      session.addDestination(queueName, true, true);
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      cm.packetProcessed(request);
//      replay(session, cm);          
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testRemoveDestination() throws Exception
//   {
//      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(queueName, true);
//      long responseTargetID = 12123;
//      request.setResponseTargetID(responseTargetID);
//      session.removeDestination(queueName, true);
//      cm.sendCommandOneway(responseTargetID, new PacketImpl(PacketImpl.NULL));
//      cm.packetProcessed(request);
//      replay(session, cm);      
//      handler.handle(123123, request);
//      verify(session, cm);
//   }
//
//   public void testUnsupportedPacket() throws Exception
//   {
//      Packet packet = EasyMock.createStrictMock(Packet.class);
//      expect(packet.getType()).andReturn(Byte.MAX_VALUE);
//      long responseTargetID = 1212;
//      EasyMock.expect(packet.getResponseTargetID()).andStubReturn(responseTargetID);
//      cm.sendCommandOneway(EasyMock.eq(responseTargetID), EasyMock.isA(PacketImpl.class));      
//      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
//      {
//         public Object answer() throws Throwable
//         {
//            MessagingExceptionMessage me = (MessagingExceptionMessage)EasyMock.getCurrentArguments()[1];
//            assertEquals(MessagingException.UNSUPPORTED_PACKET, me.getException().getCode());
//            return null;
//         }
//      });
//      cm.packetProcessed(packet);
//      replay(session, cm, packet);
//      handler.handle(1212, packet);     
//      verify(session, cm, packet);
//   }
}
