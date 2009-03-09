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

package org.jboss.messaging.tests.unit.core.client.impl;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.exception.MessagingException.OBJECT_CLOSED;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import static org.jboss.messaging.tests.util.RandomUtil.randomXid;

import java.io.File;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientProducerInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.client.impl.ConnectionManager;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.NullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ClientSessionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ClientSessionImplTest extends UnitTestCase
{

   private static final Logger log = Logger.getLogger(ClientSessionImplTest.class);

   private ConnectionManager cm;

   private RemotingConnection rc;

   private Channel channel;

   private ClientSessionImpl session;

   private boolean autoCommitSends;

   private int consumerWindowSize;

   private int consumerMaxRate;

   // Public -----------------------------------------------------------------------------------------------------------

   public void testCreateQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queueName = randomSimpleString();
      SimpleString filterString = randomSimpleString();
      boolean durable = randomBoolean();
      boolean temporary = randomBoolean();

      CreateQueueMessage request = new CreateQueueMessage(address, queueName, null, durable, false);

      // SimpleString version
      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.createQueue(address, queueName, durable);

      verifyMocks();

      // String versions
      resetMocks();

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.createQueue(address.toString(), queueName.toString(), durable);

      verifyMocks();

      // with temporary
      request = new CreateQueueMessage(address, queueName, null, durable, temporary);

      resetMocks();

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.createQueue(address.toString(), queueName.toString(), durable, temporary);

      verifyMocks();

      // full methods
      resetMocks();

      request = new CreateQueueMessage(address, queueName, filterString, durable, temporary);

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.createQueue(address, queueName, filterString, durable, temporary);

      verifyMocks();

      // full methods with String
      resetMocks();

      request = new CreateQueueMessage(address, queueName, filterString, durable, temporary);

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.createQueue(address.toString(), queueName.toString(), filterString.toString(), durable, temporary);

      verifyMocks();
   }

   public void testDeleteQueue() throws Exception
   {
      SimpleString queueName = randomSimpleString();

      SessionDeleteQueueMessage request = new SessionDeleteQueueMessage(queueName);

      // SimpleString version
      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.deleteQueue(queueName);

      verifyMocks();

      // String version
      resetMocks();
      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      replayMocks();

      session.deleteQueue(queueName.toString());

      verifyMocks();
   }

   public void testQueueQuery() throws Exception
   {
      SimpleString queueName = randomSimpleString();

      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
      SessionQueueQueryResponseMessage resp = new SessionQueueQueryResponseMessage();

      expect(channel.sendBlocking(request)).andReturn(resp);
      replayMocks();

      SessionQueueQueryResponseMessage resp2 = session.queueQuery(queueName);
      assertTrue(resp == resp2);

      verifyMocks();
   }

   public void testBindingQuery() throws Exception
   {
      SimpleString address = randomSimpleString();

      SessionBindingQueryMessage request = new SessionBindingQueryMessage(address);
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage();

      expect(channel.sendBlocking(request)).andReturn(resp);
      replayMocks();

      SessionBindingQueryResponseMessage resp2 = session.bindingQuery(request.getAddress());
      assertTrue(resp == resp2);

      verifyMocks();
   }

  
   public void testCreateConsumer() throws Exception
   {
      SimpleString queueName = randomSimpleString();
      SimpleString filterString = randomSimpleString();

      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(queueName, null, false);

      // SimpleString version
      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      SessionConsumerFlowCreditMessage flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(),
                                                                                          anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName);

      verifyMocks();

      // String version
      resetMocks();

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(), anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName.toString());

      verifyMocks();

      // with a filter
      resetMocks();

      request = new SessionCreateConsumerMessage(queueName, filterString, false);

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(), anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName, filterString);

      verifyMocks();

      // with a filter String
      resetMocks();

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(), anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName.toString(), filterString.toString());

      verifyMocks();

      // for browsing
      resetMocks();

      boolean browseOnly = randomBoolean();
      request = new SessionCreateConsumerMessage(queueName, filterString, browseOnly);

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(), anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName, filterString, browseOnly);

      verifyMocks();

      // for browsing with String
      resetMocks();

      request = new SessionCreateConsumerMessage(queueName, filterString, browseOnly);

      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(randomPositiveLong(), anyInt());
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName.toString(), filterString.toString(), browseOnly);

      verifyMocks();

      // full version
      resetMocks();

      request = new SessionCreateConsumerMessage(queueName, filterString, browseOnly);

      int maxRate = randomPositiveInt();
      int windowSize = randomPositiveInt();
      expect(channel.sendBlocking(request)).andReturn(new NullResponseMessage());
      flowMessage = new SessionConsumerFlowCreditMessage(anyLong(), windowSize);
      channel.send(flowMessage);
      replayMocks();

      session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);

      verifyMocks();

   }

   public void testCreateProducer() throws Exception
   {
      ClientProducer producer = session.createProducer();
      assertNull(producer.getAddress());

      SimpleString address = randomSimpleString();
      producer = session.createProducer(address);
      assertEquals(address, producer.getAddress());

      producer = session.createProducer(address.toString());
      assertEquals(address, producer.getAddress());

      int maxRate = randomPositiveInt();
      producer = session.createProducer(address, maxRate);
      assertEquals(address, producer.getAddress());
      assertEquals(maxRate, producer.getMaxRate());

      producer = session.createProducer(address.toString(), maxRate);
      assertEquals(address, producer.getAddress());
      assertEquals(maxRate, producer.getMaxRate());

      boolean blockOnNonPersistentSend = randomBoolean();
      boolean blockOnPersistentSend = randomBoolean();
      producer = session.createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
      assertEquals(address, producer.getAddress());
      assertEquals(maxRate, producer.getMaxRate());
      assertEquals(autoCommitSends && blockOnNonPersistentSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPersistentSend, producer.isBlockOnPersistentSend());

      producer = session.createProducer(address.toString(), maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
      assertEquals(address, producer.getAddress());
      assertEquals(maxRate, producer.getMaxRate());
      assertEquals(autoCommitSends && blockOnNonPersistentSend, producer.isBlockOnNonPersistentSend());
      assertEquals(autoCommitSends && blockOnPersistentSend, producer.isBlockOnPersistentSend());
   }

   public void testGetXAResource() throws Exception
   {
      XAResource res = session.getXAResource();

      assertTrue(res == session);
   }

   public void testAcknowledge() throws Exception
   {
      testAcknowledge(true);
      testAcknowledge(false);
   }

   public void testCommit() throws Exception
   {
      expect(channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(new NullResponseMessage());

      replayMocks();

      session.commit();

      verifyMocks();

      // if there are consumers
      resetMocks();

      ClientConsumerInternal cons = createMock(ClientConsumerInternal.class);
      expect(cons.getID()).andStubReturn(randomPositiveLong());
      cons.flushAcks();
      expect(channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT))).andReturn(new NullResponseMessage());

      replayMocks(cons);

      session.addConsumer(cons);
      session.commit();

      verifyMocks(cons);
   }

   public void testXACommit() throws Exception
   {
      testXACommit(false, false);
      testXACommit(false, true);
      testXACommit(true, false);
      testXACommit(true, true);
   }

   // public void testCleanUp2() throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   // ConnectionRegistry reg = EasyMock.createStrictMock(ConnectionRegistry.class);
   //        
   // final long sessionTargetID = 9121892;
   //                  
   // EasyMock.replay(rc, cf, pd, cm, reg);
   //      
   // ClientSessionImpl session =
   // new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
   // session.setConnectionRegistry(reg);
   //      
   // EasyMock.verify(rc, cf, pd, cm, reg);
   //      
   // EasyMock.reset(rc, cf, pd, cm, reg);
   //      
   // ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
   // ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);
   //      
   // ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
   // ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);
   //      
   // ClientBrowser browser1 = EasyMock.createStrictMock(ClientBrowser.class);
   // ClientBrowser browser2 = EasyMock.createStrictMock(ClientBrowser.class);
   //                    
   // prod1.cleanUp();
   // prod2.cleanUp();
   // cons1.cleanUp();
   // cons2.cleanUp();
   // browser1.cleanUp();
   // browser2.cleanUp();
   //      
   // cm.close();
   // final String connectionID = "uahsjash";
   // EasyMock.expect(rc.getID()).andStubReturn(connectionID);
   // reg.returnConnection(connectionID);
   //        
   // EasyMock.replay(cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm, reg, rc);
   //                 
   // session.addProducer(prod1);
   // session.addProducer(prod2);
   //      
   // session.addConsumer(cons1);
   // session.addConsumer(cons2);
   //      
   // session.addBrowser(browser1);
   // session.addBrowser(browser2);
   //      
   // assertFalse(session.isClosed());
   //      
   // session.cleanUp();
   //
   // assertTrue(session.isClosed());
   //      
   // EasyMock.verify(rc, cf, pd, prod1, prod2, cons1, cons2, browser1, browser2, cm, reg, rc);
   // }
   //         

   //   
   // public void testXAEnd() throws Exception
   // {
   // testXAEnd(XAResource.TMSUSPEND, false);
   // testXAEnd(XAResource.TMSUSPEND, true);
   // testXAEnd(XAResource.TMSUCCESS, false);
   // testXAEnd(XAResource.TMSUCCESS, true);
   // testXAEnd(XAResource.TMFAIL, false);
   // testXAEnd(XAResource.TMFAIL, true);
   // }
   //   
   // public void testXAForget() throws Exception
   // {
   // testXAForget(false);
   // testXAForget(true);
   // }
   //   
   // public void testGetTransactionTimeout() throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // Packet packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
   //
   // final int timeout = 1098289;
   //      
   // SessionXAGetTimeoutResponseMessage resp = new SessionXAGetTimeoutResponseMessage(timeout);
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // int timeout2 = session.getTransactionTimeout();
   //            
   // EasyMock.verify(rc, cf, pd, cm);
   //      
   // assertEquals(timeout, timeout2);
   // }
   //   
   // public void testIsSameRM() throws Exception
   // {
   // RemotingConnection rc1 = EasyMock.createStrictMock(RemotingConnection.class);
   // RemotingConnection rc2 = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //                               
   // EasyMock.replay(rc1, rc2, cf, pd, cm);
   //      
   // ClientSessionInternal session1 =
   // new ClientSessionImpl("blah", 4343, true, -1, false, false, false, false, rc1, cf, pd, 100, cm);
   //      
   // ClientSessionInternal session2 =
   // new ClientSessionImpl("blah2", 4343, true, -1, false, false, false, false, rc2, cf, pd, 100, cm);
   //      
   // ClientSessionInternal session3 =
   // new ClientSessionImpl("blah3", 4343, true, -1, false, false, false, false, rc2, cf, pd, 100, cm);
   //      
   // assertFalse(session1.isSameRM(session2));
   // assertFalse(session2.isSameRM(session1));
   //      
   // assertTrue(session2.isSameRM(session3));
   // assertTrue(session3.isSameRM(session2));
   //      
   // assertFalse(session1.isSameRM(session3));
   // assertFalse(session3.isSameRM(session1));
   //      
   // assertTrue(session1.isSameRM(session1));
   // assertTrue(session2.isSameRM(session2));
   // assertTrue(session3.isSameRM(session3));
   //      
   // EasyMock.verify(rc1, rc2, cf, pd, cm);
   // }
   //   
   // public void testXAPrepare() throws Exception
   // {
   // testXAPrepare(false, false);
   // testXAPrepare(false, true);
   // testXAPrepare(true, false);
   // testXAPrepare(true, true);
   // }
   //   
   // public void testXARecover() throws Exception
   // {
   // testXARecover(XAResource.TMNOFLAGS);
   // testXARecover(XAResource.TMSTARTRSCAN);
   // testXARecover(XAResource.TMENDRSCAN);
   // testXARecover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
   // }
   //   
   // public void testXARollback() throws Exception
   // {
   // testXARollback(true);
   // testXARollback(false);
   // }
   //   
   // public void testXASetTransactionTimeout() throws Exception
   // {
   // testXASetTransactionTimeout(false);
   // testXASetTransactionTimeout(true);
   // }
   //   
   // public void testXAStart() throws Exception
   // {
   // testXAStart(XAResource.TMJOIN, false);
   // testXAStart(XAResource.TMRESUME, false);
   // testXAStart(XAResource.TMNOFLAGS, false);
   // testXAStart(XAResource.TMJOIN, true);
   // testXAStart(XAResource.TMRESUME, true);
   // testXAStart(XAResource.TMNOFLAGS, true);
   // }
   //
   // public void testCleanUp1() throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   // ConnectionRegistry reg = EasyMock.createStrictMock(ConnectionRegistry.class);
   //
   // CreateQueueMessage request = new CreateQueueMessage(new SimpleString("blah"), new
   // SimpleString("hagshg"),
   // new SimpleString("jhjhs"), false, false);
   //
   // final int targetID = 121;
   //
   // EasyMock.expect(cm.sendCommandBlocking(targetID, request)).andReturn(null);
   //
   // EasyMock.replay(rc, cf, pd, cm);
   //
   // ClientSessionImpl session = new ClientSessionImpl("blah", targetID, false, -1, false, false, false, false, rc, cf,
   // pd, 100, cm);
   // session.setConnectionRegistry(reg);
   //
   // session.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(),
   // request.isDurable());
   //
   // EasyMock.verify(rc, cf, pd, cm);
   //
   // EasyMock.reset(rc, cf, pd, cm);
   // cm.close();
   // final String connectionID = "uahsjash";
   // EasyMock.expect(rc.getID()).andStubReturn(connectionID);
   // reg.returnConnection(connectionID);
   // EasyMock.replay(rc, cf, pd, cm);
   // session.cleanUp();
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //
   // public void notXA() throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //              
   // final long sessionTargetID = 9121892;
   //                  
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, false, -1, false, false, false,
   // false, rc, cf, pd, 100, cm);
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   //      
   // EasyMock.reset(rc, cf, pd, cm);
   // try
   // {
   // session.commit(randomXid(), false);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.end(randomXid(), 8778);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.forget(randomXid());
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.getTransactionTimeout();
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.isSameRM(session);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.prepare(randomXid());
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.recover(89787);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.rollback(randomXid());
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.setTransactionTimeout(767);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   //      
   // try
   // {
   // session.start(randomXid(), 8768);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   //   
   // public void testCreateMessage() throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // MessagingBuffer buff = EasyMock.createMock(MessagingBuffer.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   // EasyMock.expect(rc.createBuffer(ClientSessionImpl.INITIAL_MESSAGE_BODY_SIZE)).andStubReturn(buff);
   // EasyMock.replay(rc);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", 453543, false, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // ClientMessage msg = session.createClientMessage(false);
   // assertFalse(msg.isDurable());
   //      
   // msg = session.createClientMessage(true);
   // assertTrue(msg.isDurable());
   //      
   // final byte type = 123;
   //      
   // msg = session.createClientMessage(type, false);
   // assertFalse(msg.isDurable());
   // assertEquals(type, msg.getType());
   //      
   // msg = session.createClientMessage(type, true);
   // assertTrue(msg.isDurable());
   // assertEquals(type, msg.getType());
   //            
   // final long expiration = 120912902;
   // final long timestamp = 1029128;
   // final byte priority = 12;
   //            
   // msg = session.createClientMessage(type, false, expiration, timestamp, priority);
   // assertFalse(msg.isDurable());
   // assertEquals(type, msg.getType());
   // assertEquals(expiration, msg.getExpiration());
   // assertEquals(timestamp, msg.getTimestamp());
   // assertEquals(priority, msg.getPriority());
   //
   // msg = session.createClientMessage(type, true, expiration, timestamp, priority);
   // assertTrue(msg.isDurable());
   // assertEquals(type, msg.getType());
   // assertEquals(expiration, msg.getExpiration());
   // assertEquals(timestamp, msg.getTimestamp());
   // assertEquals(priority, msg.getPriority());
   // }
   //   
   // // Private -------------------------------------------------------------------------------------------
   //
   public void testClose() throws Exception
   {
      ClientProducerInternal prod1 = EasyMock.createStrictMock(ClientProducerInternal.class);
      ClientProducerInternal prod2 = EasyMock.createStrictMock(ClientProducerInternal.class);

      ClientConsumerInternal cons1 = EasyMock.createStrictMock(ClientConsumerInternal.class);
      ClientConsumerInternal cons2 = EasyMock.createStrictMock(ClientConsumerInternal.class);

      prod1.close();
      prod2.close();
      expect(cons1.getID()).andStubReturn(randomPositiveLong());
      cons1.close();
      expect(cons2.getID()).andStubReturn(randomPositiveLong());
      cons2.close();

      expect(channel.sendBlocking(new SessionCloseMessage())).andReturn(new NullResponseMessage());

      channel.close();
      expect(rc.removeFailureListener(session)).andReturn(true);
      cm.removeSession(session);

      replayMocks(prod1, prod2, cons1, cons2);

      session.addProducer(prod1);
      session.addProducer(prod2);

      session.addConsumer(cons1);
      session.addConsumer(cons2);

      assertFalse(session.isClosed());

      session.close();

      assertTrue(session.isClosed());

      verifyMocks(prod1, prod2, cons1, cons2);

   }

   public void testCloseAClosedSession() throws Exception
   {
      expect(channel.sendBlocking(new SessionCloseMessage())).andReturn(new NullResponseMessage());

      channel.close();
      expect(rc.removeFailureListener(session)).andReturn(true);
      cm.removeSession(session);

      replayMocks();

      assertFalse(session.isClosed());

      session.close();

      assertTrue(session.isClosed());

      session.close(); // DO NOTHING

      verifyMocks();
   }

   public void testForbiddenCallsOnClosedSession() throws Exception
   {
      expect(channel.sendBlocking(new SessionCloseMessage())).andReturn(new NullResponseMessage());

      channel.close();
      expect(rc.removeFailureListener(session)).andReturn(true);
      cm.removeSession(session);

      replayMocks();

      assertFalse(session.isClosed());

      session.close();

      assertTrue(session.isClosed());

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.start();
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.stop();
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.createQueue(randomSimpleString(), randomSimpleString(), randomBoolean());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.deleteQueue(randomSimpleString());
         }
      });

     
      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.queueQuery(randomSimpleString());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.bindingQuery(randomSimpleString());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.createConsumer(randomSimpleString());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.createFileConsumer(new File("blah"), randomSimpleString());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.createProducer();
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.commit();
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.rollback();
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.acknowledge(randomPositiveLong(), randomPositiveLong());
         }
      });

      assertMessagingException(OBJECT_CLOSED, new SessionCaller()
      {
         public void call(ClientSessionImpl session) throws MessagingException
         {
            session.expire(randomPositiveLong(), randomPositiveLong());
         }
      });
      verifyMocks();
   }

   //   
   // private void testXAStart(int flags, boolean error) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // Xid xid = randomXid();
   //      
   // Packet packet = null;
   // if (flags == XAResource.TMJOIN)
   // {
   // packet = new SessionXAJoinMessage(xid);
   // }
   // else if (flags == XAResource.TMRESUME)
   // {
   // packet = new SessionXAResumeMessage(xid);
   // }
   // else if (flags == XAResource.TMNOFLAGS)
   // {
   // packet = new SessionXAStartMessage(xid);
   // }
   //
   // final int numMessages = 10;
   //      
   // if (flags != XAResource.TMNOFLAGS)
   // {
   // SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
   //         
   // cm.sendCommandOneway(sessionTargetID, msg);
   // }
   //      
   // SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false,
   // false, rc, cf, pd, 100, cm);
   //      
   // //Simulate some unflushed messages
   //      
   // for (int i = 0; i < numMessages; i++)
   // {
   // session.delivered(i, false);
   // session.acknowledge();
   // }
   //      
   // if (error)
   // {
   // try
   // {
   // session.start(xid, flags);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   // else
   // {
   // session.start(xid, flags);
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXASetTransactionTimeout(boolean error) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // final int timeout = 1897217;
   //      
   // SessionXASetTimeoutMessage packet = new SessionXASetTimeoutMessage(timeout);
   //      
   // SessionXASetTimeoutResponseMessage resp = new SessionXASetTimeoutResponseMessage(!error);
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // boolean ok = session.setTransactionTimeout(timeout);
   //      
   // assertTrue(ok == !error);
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXARollback(boolean error) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //           
   // final long sessionTargetID = 9121892;
   //      
   // Xid xid = randomXid();
   //      
   // SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);
   //      
   // SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // if (error)
   // {
   // try
   // {
   // session.rollback(xid);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   // else
   // {
   // session.rollback(xid);
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXARecover(final int flags) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //      
   // final long sessionTargetID = 9121892;
   //      
   // final Xid[] xids = new Xid[] { randomXid(), randomXid(), randomXid() } ;
   //                  
   // if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
   // {
   // PacketImpl packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
   //         
   // SessionXAGetInDoubtXidsResponseMessage resp = new SessionXAGetInDoubtXidsResponseMessage(Arrays.asList(xids));
   //         
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   // }
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // Xid[] xids2 = session.recover(flags);
   //      
   // if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
   // {
   // assertEquals(xids.length, xids2.length);
   //         
   // for (int i = 0; i < xids.length; i++)
   // {
   // assertEquals(xids[i], xids2[i]);
   // }
   // }
   // else
   // {
   // assertTrue(xids2.length == 0);
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXAPrepare(boolean error, boolean readOnly) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // Xid xid = randomXid();
   //      
   // SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);
   //      
   // SessionXAResponseMessage resp = new SessionXAResponseMessage(error, error ? XAException.XAER_RMERR : readOnly ?
   // XAResource.XA_RDONLY : XAResource.XA_OK, "blah");
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // if (error)
   // {
   // try
   // {
   // session.prepare(xid);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   // else
   // {
   // int res = session.prepare(xid);
   //         
   // if (readOnly)
   // {
   // assertEquals(XAResource.XA_RDONLY, res);
   // }
   // else
   // {
   // assertEquals(XAResource.XA_OK, res);
   // }
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXAForget(final boolean error) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // Xid xid = randomXid();
   //      
   // Packet packet = new SessionXAForgetMessage(xid);
   //
   // final int numMessages = 10;
   //      
   // SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
   //      
   // cm.sendCommandOneway(sessionTargetID, msg);
   //      
   // SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session =
   // new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false, false, rc, cf, pd, 100, cm);
   //      
   // //Simulate some unflushed messages
   //      
   // for (int i = 0; i < numMessages; i++)
   // {
   // session.delivered(i, false);
   // session.acknowledge();
   // }
   //      
   // if (error)
   // {
   // try
   // {
   // session.forget(xid);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   // else
   // {
   // session.forget(xid);
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   // private void testXAEnd(int flags, boolean error) throws Exception
   // {
   // RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
   // ClientSessionFactory cf = EasyMock.createStrictMock(ClientSessionFactory.class);
   // PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
   // CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
   //        
   // final long sessionTargetID = 9121892;
   //      
   // Xid xid = randomXid();
   //      
   // Packet packet = null;
   // if (flags == XAResource.TMSUSPEND)
   // {
   // packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
   // }
   // else if (flags == XAResource.TMSUCCESS)
   // {
   // packet = new SessionXAEndMessage(xid, false);
   // }
   // else if (flags == XAResource.TMFAIL)
   // {
   // packet = new SessionXAEndMessage(xid, true);
   // }
   //
   // final int numMessages = 10;
   //      
   // SessionAcknowledgeMessageBlah msg = new SessionAcknowledgeMessageBlah(numMessages - 1, true);
   //      
   // cm.sendCommandOneway(sessionTargetID, msg);
   //      
   // SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, "blah");
   //      
   // EasyMock.expect(cm.sendCommandBlocking(sessionTargetID, packet)).andReturn(resp);
   //                       
   // EasyMock.replay(rc, cf, pd, cm);
   //      
   // ClientSessionInternal session = new ClientSessionImpl("blah", sessionTargetID, true, -1, false, false, false,
   // false,rc, cf, pd, 100, cm);
   //      
   // //Simulate some unflushed messages
   //      
   // for (int i = 0; i < numMessages; i++)
   // {
   // session.delivered(i, false);
   // session.acknowledge();
   // }
   //      
   // if (error)
   // {
   // try
   // {
   // session.end(xid, flags);
   // fail("Should throw exception");
   // }
   // catch (XAException e)
   // {
   // assertEquals(XAException.XAER_RMERR, e.errorCode);
   // }
   // }
   // else
   // {
   // session.end(xid, flags);
   // }
   //      
   // EasyMock.verify(rc, cf, pd, cm);
   // }
   //   
   private void testXACommit(final boolean onePhase, boolean error) throws Exception
   {
      resetMocks();

      final Xid xid = randomXid();

      SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);
      SessionXAResponseMessage resp = new SessionXAResponseMessage(error, XAException.XAER_RMERR, randomString());

      expect(channel.sendBlocking(packet)).andReturn(resp);
      replayMocks();

      if (error)
      {
         assertXAException(XAException.XAER_RMERR, new SessionCaller()
         {
            public void call(ClientSessionImpl session) throws Exception
            {
               session.commit(xid, onePhase);
            }
         });
      }
      else
      {
         session.commit(xid, onePhase);
      }

      verifyMocks();
   }

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      cm = EasyMock.createMock(ConnectionManager.class);
      rc = createMock(RemotingConnection.class);
      channel = createMock(Channel.class);

      autoCommitSends = randomBoolean();
      consumerWindowSize = randomPositiveInt();
      consumerMaxRate = randomPositiveInt();
      session = new ClientSessionImpl(cm,
                                      randomString(),
                                      true,
                                      autoCommitSends,
                                      true,
                                      false,
                                      true,
                                      true,
                                      1,
                                      consumerWindowSize,
                                      consumerMaxRate,
                                      1,
                                      true,
                                      true,
                                      1,
                                      rc,
                                      1,
                                      channel);
   }

   @Override
   protected void tearDown() throws Exception
   {
      cm = null;
      rc = null;
      channel = null;
      session = null;

      super.tearDown();
   }

   // Private -----------------------------------------------------

   private void testAcknowledge(boolean blockOnAck) throws Exception
   {
      resetMocks();

      long messageID = randomPositiveLong();
      long consumerID = randomPositiveLong();

      boolean preack = false;
      session = new ClientSessionImpl(cm,
                                      randomString(),
                                      true,
                                      autoCommitSends,
                                      true,
                                      preack,
                                      blockOnAck,
                                      true,
                                      1,
                                      consumerWindowSize,
                                      consumerMaxRate,
                                      1,
                                      true,
                                      true,
                                      1,
                                      rc,
                                      1,
                                      channel);

      if (blockOnAck)
      {
         expect(channel.sendBlocking(new SessionAcknowledgeMessage(consumerID, messageID, true))).andReturn(new NullResponseMessage());
      }
      else
      {
         channel.send(new SessionAcknowledgeMessage(consumerID, messageID, false));
      }

      replayMocks();

      session.acknowledge(consumerID, messageID);

      verifyMocks();
   }

   private void replayMocks(Object... additionalMocks)
   {
      replay(cm, rc, channel);
      replay(additionalMocks);
   }

   private void resetMocks(Object... additionalMocks)
   {
      reset(cm, rc, channel);
      reset(additionalMocks);
   }

   private void verifyMocks(Object... additionalMocks)
   {
      verify(cm, rc, channel);
      verify(additionalMocks);
   }

   private void assertMessagingException(int errorCode, SessionCaller caller) throws Exception
   {
      try
      {
         caller.call(session);
         fail("Should throw a MessagingException");
      }
      catch (MessagingException e)
      {
         assertEquals(errorCode, e.getCode());
      }
   }

   private void assertXAException(int errorCode, SessionCaller caller) throws Exception
   {
      try
      {
         caller.call(session);
         fail("Should throw a XAException");
      }
      catch (XAException e)
      {
         assertEquals(errorCode, e.errorCode);
      }
   }

   // Inner classes -------------------------------------------------

   public interface SessionCaller
   {
      void call(ClientSessionImpl session) throws Exception;
   }

}
