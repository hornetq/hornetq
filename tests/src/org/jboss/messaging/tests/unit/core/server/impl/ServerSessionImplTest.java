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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.Delivery;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.core.server.impl.ServerBrowserImpl;
import org.jboss.messaging.core.server.impl.ServerSessionImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * A ServerSessionImplTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerSessionImplTest extends UnitTestCase
{
   private ServerConnection conn;
   private RemotingConnection returner;
   private StorageManager sm;
   private PostOffice po;
   private HierarchicalRepository<QueueSettings> qs;
   private ResourceManager rm;
   private SecurityStore ss;
   private PacketDispatcher pd;
   private Executor executor;

   public void testConstructor() throws Exception
   {
      testConstructor(false);
      testConstructor(true);
   }

   public void testAutoCommitSend() throws Exception
   {
      testAutoCommitSend(false);

      testAutoCommitSend(true);
   }

   public void testNotAutoCommitSend() throws Exception
   {
      testNotAutoCommitSend(false);

      testNotAutoCommitSend(true);
   }

   public void testAutoCommitSendFailsSecurity() throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerMessage msg = createStrictMock(ServerMessage.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);
      expect(sm.generateTransactionID()).andReturn(71626L);

      final SimpleString dest = new SimpleString("blah");
      expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      expectLastCall().andThrow(new MessagingException(MessagingException.SECURITY_EXCEPTION));

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);

      ServerSessionImpl session = new ServerSessionImpl(conn, true, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      try
      {
         session.send(msg);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         //Ok
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);
   }

   public void testNotAutoCommitSendFailsSecurity() throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerMessage msg = createStrictMock(ServerMessage.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);
      expect(sm.generateTransactionID()).andReturn(71626L);

      final SimpleString dest = new SimpleString("blah");
      expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      expectLastCall().andThrow(new MessagingException(MessagingException.SECURITY_EXCEPTION));


      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);

      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      try
      {
         session.send(msg);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         //Ok
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);

      assertEquals(Transaction.State.ROLLBACK_ONLY, session.getTransaction().getState());
   }

   public void testAcknowledgeAllUpToAutoCommitAck() throws Exception
   {
      testAcknowledgeAllUpToAutoCommitAck(false, false);
      testAcknowledgeAllUpToAutoCommitAck(false, true);
      testAcknowledgeAllUpToAutoCommitAck(true, false);
      testAcknowledgeAllUpToAutoCommitAck(true, true);
   }

   public void testAcknowledgeAllUpToNotAutoCommitAck() throws Exception
   {
      testAcknowledgeAllUpToNotAutoCommitAck(false, false);
      testAcknowledgeAllUpToNotAutoCommitAck(false, true);
      testAcknowledgeAllUpToNotAutoCommitAck(true, false);
      testAcknowledgeAllUpToNotAutoCommitAck(true, true);
   }

   public void testAcknowledgeNotAllUpToAutoCommitAck() throws Exception
   {
      testAcknowledgeNotAllUpToAutoCommitAck(false, false);
      testAcknowledgeNotAllUpToAutoCommitAck(false, true);
      testAcknowledgeNotAllUpToAutoCommitAck(true, false);
      testAcknowledgeNotAllUpToAutoCommitAck(true, true);
   }

   public void testAcknowledgeNotAllUpToNotAutoCommitAck() throws Exception
   {
      testAcknowledgeNotAllUpToNotAutoCommitAck(false, false);
      testAcknowledgeNotAllUpToNotAutoCommitAck(false, true);
      testAcknowledgeNotAllUpToNotAutoCommitAck(true, false);
      testAcknowledgeNotAllUpToNotAutoCommitAck(true, true);
   }

   public void testCreateBrowserNullFilter() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(binding.getQueue()).andReturn(queue);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateBrowserResponseMessage message = session.createBrowser(address, null);
      assertEquals(message.getBrowserTargetID(), 2);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateBrowseFilter() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      SimpleString filter = new SimpleString("myprop = 'foo'");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(binding.getQueue()).andReturn(queue);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateBrowserResponseMessage message = session.createBrowser(address, filter);
      assertEquals(message.getBrowserTargetID(), 2);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateBrowseNullBinding() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      SimpleString filter = new SimpleString("myprop = 'foo'");
      expect(po.getBinding(address)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.createBrowser(address, filter);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testCreateAndRemoveBrowserDoesntExist() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(binding.getQueue()).andReturn(queue);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      expect(pd.generateID()).andReturn(3l);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateBrowserResponseMessage message = session.createBrowser(address, null);
      try
      {
         session.removeBrowser(new ServerBrowserImpl(session, queue, null, pd, returner));
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      assertEquals(message.getBrowserTargetID(), 2);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateProducer() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateProducerResponseMessage message = session.createProducer(99l, address, -1, 99);
      assertEquals(message.getInitialCredits(), -1);
      assertEquals(message.getMaxRate(), 99);
      assertEquals(message.getProducerTargetID(), 2l);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateProducerWithFlowController() throws Exception
   {
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      FlowController flowController = createStrictMock(FlowController.class);
      Binding binding = createStrictMock(Binding.class);
      expect(po.getFlowController(address)).andReturn(flowController);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      expect(flowController.getInitialCredits(eq(999), (ServerProducer) anyObject())).andReturn(12345);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, flowController);
      SessionCreateProducerResponseMessage message = session.createProducer(99l, address, 999, 99);
      assertEquals(message.getInitialCredits(), 12345);
      assertEquals(message.getMaxRate(), 99);
      assertEquals(message.getProducerTargetID(), 2l);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, flowController);
   }

   public void testRemoveProducerDoesntExist() throws Exception
   {
      ServerSessionImpl session = create(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.removeProducer(createStrictMock(ServerProducer.class));
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testCreateConsumer() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateConsumerResponseMessage message = session.createConsumer(1, address, null, true, true, -1, 99);
      assertEquals(message.getWindowSize(), -1);
      assertEquals(message.getConsumerTargetID(), 2l);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateConsumerWithQueueSettings() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setConsumerMaxRate(66);
      queueSettings.setConsumerWindowSize(55);
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateConsumerResponseMessage message = session.createConsumer(1, address, null, true, true, 999, 99);
      assertEquals(message.getWindowSize(), 55);
      assertEquals(message.getConsumerTargetID(), 2l);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateConsumerWithFilter() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setConsumerMaxRate(66);
      queueSettings.setConsumerWindowSize(55);
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      SimpleString filter = new SimpleString("myprop = 'fdfdf'");
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      SessionCreateConsumerResponseMessage message = session.createConsumer(1, address, filter, true, true, 999, 99);
      assertEquals(message.getWindowSize(), 55);
      assertEquals(message.getConsumerTargetID(), 2l);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testRemoveConsumerDoesntExist() throws Exception
   {
      ServerSessionImpl session = create(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.removeConsumer(createNiceMock(ServerConsumer.class));
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }


   public void testCreateConsumerNoQueueThrowsException() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      expect(po.getBinding(address)).andReturn(null);
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.createConsumer(1, address, null, true, true, -1, 99);
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testSetStarted() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getAddress()).andReturn(address);
      ss.check(address, CheckType.READ, conn);
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      queue.addConsumer((Consumer) anyObject());
      queue.deliverAsync(executor);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      //we need to create a consumer to verify it gets started
      SessionCreateConsumerResponseMessage message = session.createConsumer(1, address, null, true, true, -1, 99);
      session.setStarted(true);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testSetClose() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andStubReturn(binding);
      expect(binding.getAddress()).andStubReturn(address);
      ss.check(address, CheckType.READ, conn);
      expectLastCall().asStub();
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      expect(pd.generateID()).andReturn(3l);
      queue.addConsumer((Consumer) anyObject());
      pd.register((PacketHandler) anyObject());
      expect(pd.generateID()).andReturn(4l);
      pd.register((PacketHandler) anyObject());
      expect(queue.removeConsumer((Consumer) anyObject())).andReturn(true);
      expect(queue.getConsumerCount()).andStubReturn(1);
      pd.unregister(2);
      pd.unregister(4);
      pd.unregister(3);
      expect(sm.generateTransactionID()).andReturn(6l);
      conn.removeSession(session);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      //we need to create a consumer to verify they get closed
      session.createConsumer(1, address, null, true, true, -1, 99);
      session.createProducer(99l, address, -1, 99);
      session.createBrowser(address, null);
      session.close();
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testRollback() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      expect(consumer.getClientTargetID()).andStubReturn(999l);
      MessageReference[] references = new MessageReference[10];
      ServerMessage[] messages = new ServerMessage[10];

      Queue queue = createStrictMock(Queue.class);
      for (int i = 0; i < 10; i++)
      {
         references[i] = createStrictMock(MessageReference.class);
         messages[i] = createStrictMock(ServerMessage.class);
         expect(messages[i].isDurable()).andStubReturn(false);
         expect(references[i].getMessage()).andStubReturn(messages[i]);
         expect(references[i].getDeliveryCount()).andReturn(0);
         expect(references[i].getQueue()).andReturn(queue);
      }
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      expect(po.getBinding(address)).andStubReturn(binding);
      expect(binding.getAddress()).andStubReturn(address);
      ss.check(address, CheckType.READ, conn);
      expectLastCall().asStub();
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      queue.addConsumer((Consumer) anyObject());
      for (int i = 0; i < 10; i++)
      {
         returner.sendOneWay((Packet) anyObject());
      }

      queue.lock();
      for (int i = 0; i < 10; i++)
      {
         expect(references[i].cancel(sm, po, qs)).andReturn(true);
      }
      queue.addListFirst((LinkedList<MessageReference>) anyObject());
      queue.unlock();

      expect(sm.generateTransactionID()).andReturn(65l);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      replay((Object[]) references);
      replay((Object[]) messages);
      //we need to create a consumer to verify things get rolled back
      session.createConsumer(1, address, null, true, true, -1, 99);
      for (MessageReference reference : references)
      {
         session.handleDelivery(reference, consumer);
      }
      session.rollback();
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      verify((Object[]) references);
      verify((Object[]) messages);
   }

   public void testCancelAll() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      expect(consumer.getClientTargetID()).andStubReturn(999l);
      MessageReference[] references = new MessageReference[10];
      ServerMessage[] messages = new ServerMessage[10];

      Queue queue = createStrictMock(Queue.class);
      for (int i = 0; i < 10; i++)
      {
         references[i] = createStrictMock(MessageReference.class);
         messages[i] = createStrictMock(ServerMessage.class);
         expect(messages[i].isDurable()).andStubReturn(false);
         expect(references[i].getMessage()).andStubReturn(messages[i]);
         expect(references[i].getDeliveryCount()).andReturn(0);
         expect(references[i].getQueue()).andReturn(queue);
      }
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      expect(po.getBinding(address)).andStubReturn(binding);
      expect(binding.getAddress()).andStubReturn(address);
      ss.check(address, CheckType.READ, conn);
      expectLastCall().asStub();
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      queue.addConsumer((Consumer) anyObject());
      for (int i = 0; i < 10; i++)
      {
         returner.sendOneWay((Packet) anyObject());
      }

      queue.lock();
      for (int i = 0; i < 10; i++)
      {
         expect(references[i].cancel(sm, po, qs)).andReturn(true);
      }
      queue.addListFirst((LinkedList<MessageReference>) anyObject());
      queue.unlock();

      expect(sm.generateTransactionID()).andReturn(65l);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      replay((Object[]) references);
      replay((Object[]) messages);
      //we need to create a consumer to verify things get rolled back
      session.createConsumer(1, address, null, true, true, -1, 99);
      for (MessageReference reference : references)
      {
         session.handleDelivery(reference, consumer);
      }
      session.cancel(-1, false);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      verify((Object[]) references);
      verify((Object[]) messages);
   }

   public void testCancelOne() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      expect(consumer.getClientTargetID()).andStubReturn(999l);
      MessageReference[] references = new MessageReference[10];
      ServerMessage[] messages = new ServerMessage[10];

      Queue queue = createStrictMock(Queue.class);
      for (int i = 0; i < 10; i++)
      {
         references[i] = createStrictMock(MessageReference.class);
         messages[i] = createStrictMock(ServerMessage.class);
         expect(messages[i].isDurable()).andStubReturn(false);
         expect(references[i].getMessage()).andStubReturn(messages[i]);
         expect(references[i].getDeliveryCount()).andReturn(0);
         //expect(references[i].getQueue()).andReturn(queue);
      }
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      expect(po.getBinding(address)).andStubReturn(binding);
      expect(binding.getAddress()).andStubReturn(address);
      ss.check(address, CheckType.READ, conn);
      expectLastCall().asStub();
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      queue.addConsumer((Consumer) anyObject());
      for (int i = 0; i < 10; i++)
      {
         returner.sendOneWay((Packet) anyObject());
      }

      references[5].expire(sm, po, qs);

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      replay((Object[]) references);
      replay((Object[]) messages);
      session.createConsumer(1, address, null, true, true, -1, 99);
      for (MessageReference reference : references)
      {
         session.handleDelivery(reference, consumer);
      }
      session.cancel(5, true);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      verify((Object[]) references);
      verify((Object[]) messages);
   }

   public void testCommit() throws Exception
   {
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);
      expect(consumer.getClientTargetID()).andStubReturn(999l);
      MessageReference[] references = new MessageReference[10];
      ServerMessage[] messages = new ServerMessage[10];

      Queue queue = createStrictMock(Queue.class);
      for (int i = 0; i < 10; i++)
      {
         references[i] = createStrictMock(MessageReference.class);
         messages[i] = createStrictMock(ServerMessage.class);
         expect(messages[i].isDurable()).andStubReturn(false);
         expect(references[i].getMessage()).andStubReturn(messages[i]);
         expect(references[i].getDeliveryCount()).andReturn(0);
         references[i].incrementDeliveryCount();
         expect(references[i].getQueue()).andStubReturn(queue);
      }
      QueueSettings queueSettings = new QueueSettings();
      SimpleString address = new SimpleString("qName");
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      expect(po.getBinding(address)).andStubReturn(binding);
      expect(binding.getAddress()).andStubReturn(address);
      ss.check(address, CheckType.READ, conn);
      expectLastCall().asStub();
      expect(qs.getMatch("qName")).andStubReturn(queueSettings);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(conn.getID()).andReturn(55l);
      expect(conn.isStarted()).andReturn(true);
      expect(pd.generateID()).andReturn(2l);
      pd.register((PacketHandler) anyObject());
      queue.addConsumer((Consumer) anyObject());
      for (int i = 0; i < 10; i++)
      {
         returner.sendOneWay((Packet) anyObject());
         queue.referenceAcknowledged(references[i]);
      }
      expect(sm.generateTransactionID()).andReturn(10l);

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      replay((Object[]) references);
      replay((Object[]) messages);
      session.createConsumer(1, address, null, true, true, -1, 99);
      for (MessageReference reference : references)
      {
         session.handleDelivery(reference, consumer);
      }
      session.acknowledge(10, true);
      session.commit();
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, consumer);
      verify((Object[]) references);
      verify((Object[]) messages);
   }

   public void testXAStart() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(sm.generateTransactionID()).andReturn(1l);
      expect(rm.putTransaction(eq(xid), (Transaction) anyObject())).andReturn(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAStart(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAStartNoId() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(sm.generateTransactionID()).andReturn(1l);
      expect(rm.putTransaction(eq(xid), (Transaction) anyObject())).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAStart(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_DUPID);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAStartTXExists() throws Exception
   {
      ServerSessionImpl session = create(false);
      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAStart(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXACommit() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      tx.commit();
      expect(rm.removeTransaction(xid)).andReturn(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XACommit(true, xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXACommitNotRemoved() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      tx.commit();
      expect(rm.removeTransaction(xid)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XACommit(true, xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXACommitStateSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XACommit(true, xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXACommitNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XACommit(true, xid);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXACommitTxExists() throws Exception
   {
      ServerSessionImpl session = create(false);
      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XACommit(true, xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAEnd() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      tx.resume();
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAEnd(xid, false);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAEndNotSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAEnd(xid, false);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAEndNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);

      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAEnd(xid, false);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAEndTxists() throws Exception
   {
      ServerSessionImpl session = create(true);

      Xid xid = createStrictMock(Xid.class);
      expect(sm.generateTransactionID()).andReturn(9l);
      expect(rm.putTransaction(eq(xid), (Transaction) anyObject())).andAnswer(new IAnswer<Boolean>()
      {
         public Boolean answer() throws Throwable
         {
            TransactionImpl tx = (TransactionImpl) getCurrentArguments()[1];
            tx.suspend();
            return true;
         }
      });
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      session.XAStart(xid);
      SessionXAResponseMessage message = session.XAEnd(xid, false);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAForget() throws Exception
   {
      ServerSessionImpl session = create(true);

      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAForget(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAJoin() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAJoin(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAJoinSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAJoin(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAJoinNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAJoin(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAPrepare() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      expect(tx.isEmpty()).andReturn(false);
      tx.prepare();
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAPrepareTxEmpty() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      expect(tx.isEmpty()).andReturn(true);
      expect(rm.removeTransaction(xid)).andReturn(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_RDONLY);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAPrepareTxEmptyNotRemoved() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      expect(tx.isEmpty()).andReturn(true);
      expect(rm.removeTransaction(xid)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAPrepareSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAPrepareNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAPrepareTxExists() throws Exception
   {
      ServerSessionImpl session = create(false);
      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAPrepare(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAResume() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      tx.resume();
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAResume(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAResumeNotSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XAResume(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXAResumeNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAResume(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXAResumeTxExists() throws Exception
   {
      ServerSessionImpl session = create(false);
      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XAResume(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXARollback() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      expect(rm.removeTransaction(xid)).andReturn(true);
      tx.rollback(qs);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XARollback(xid);
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXARollbackTxNotRemoved() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      expect(rm.removeTransaction(xid)).andReturn(false);
      tx.rollback(qs);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XARollback(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXARollbackTxSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      SessionXAResponseMessage message = session.XARollback(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXARollbackNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XARollback(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_NOTA);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXARollbackTxExists() throws Exception
   {
      ServerSessionImpl session = create(false);
      Xid xid = createStrictMock(Xid.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      SessionXAResponseMessage message = session.XARollback(xid);
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXASuspend() throws Exception
   {
      ServerSessionImpl session = create(true);
      Transaction tx = createStrictMock(Transaction.class);
      Xid xid = createStrictMock(Xid.class);
      expect(rm.getTransaction(xid)).andReturn(tx);
      expect(tx.getState()).andReturn(Transaction.State.SUSPENDED);
      tx.resume();
      expect(tx.getState()).andReturn(Transaction.State.ACTIVE);
      tx.suspend();
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
      //calling resume first will allow us to get the mock tx in
      session.XAResume(xid);
      SessionXAResponseMessage message = session.XASuspend();
      assertEquals(message.getResponseCode(), XAResource.XA_OK);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid, tx);
   }

   public void testXASuspendSuspended() throws Exception
   {
      ServerSessionImpl session = create(true);

      Xid xid = createStrictMock(Xid.class);
      expect(sm.generateTransactionID()).andReturn(9l);
      expect(rm.putTransaction(eq(xid), (Transaction) anyObject())).andAnswer(new IAnswer<Boolean>()
      {
         public Boolean answer() throws Throwable
         {
            TransactionImpl tx = (TransactionImpl) getCurrentArguments()[1];
            tx.suspend();
            return true;
         }
      });
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
      session.XAStart(xid);
      SessionXAResponseMessage message = session.XASuspend();
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, xid);
   }

   public void testXASuspendNoTx() throws Exception
   {
      ServerSessionImpl session = create(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      SessionXAResponseMessage message = session.XASuspend();
      assertEquals(message.getResponseCode(), XAException.XAER_PROTO);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testGetInDoubtXids() throws Exception
   {
      ServerSessionImpl session = create(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      assertNull(session.getInDoubtXids());
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testSetXaTimeoutTrue() throws Exception
   {
      ServerSessionImpl session = create(true);
      expect(rm.setTimeoutSeconds(123456789)).andReturn(true);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      assertTrue(session.setXATimeout(123456789));
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testSetXaTimeoutFalse() throws Exception
   {
      ServerSessionImpl session = create(true);
      expect(rm.setTimeoutSeconds(123456789)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      assertFalse(session.setXATimeout(123456789));
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testGetXaTimeout() throws Exception
   {
      ServerSessionImpl session = create(true);
      expect(rm.getTimeoutSeconds()).andReturn(987654321);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      assertEquals(session.getXATimeout(), 987654321);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testAddDestination() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      ss.check(address, CheckType.CREATE, conn);
      expect(po.addDestination(address, true)).andReturn(true);
      conn.addTemporaryDestination(address);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      session.addDestination(address, true);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testAddDestinationThrowsExceptionOnQNotAdded() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      ss.check(address, CheckType.CREATE, conn);
      expect(po.addDestination(address, true)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.addDestination(address, true);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.ADDRESS_EXISTS);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testRemoveDestination() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      ss.check(address, CheckType.CREATE, conn);
      expect(po.removeDestination(address, true)).andReturn(true);
      conn.removeTemporaryDestination(address);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      session.removeDestination(address, true);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testRemoveDestinationThrowsExceptionOnQNotAdded() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      ss.check(address, CheckType.CREATE, conn);
      expect(po.removeDestination(address, true)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      try
      {
         session.removeDestination(address, true);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.ADDRESS_DOES_NOT_EXIST);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
   }

   public void testCreateQueueNoBinding() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      expect(po.containsDestination(address)).andReturn(false);
      ss.check(address, CheckType.CREATE, conn);
      expect(po.getBinding(address)).andReturn(binding);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      try
      {
         session.createQueue(address, address, null, true, false);
         fail("shoul throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.QUEUE_EXISTS);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateQueueNoSecurity() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      expect(po.containsDestination(address)).andReturn(false);
      ss.check(address, CheckType.CREATE, conn);
      expectLastCall().andThrow(new MessagingException(MessagingException.SECURITY_EXCEPTION));
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      try
      {
         session.createQueue(address, address, null, true, false);
         fail("shoul throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.SECURITY_EXCEPTION);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateQueueTemporary() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.containsDestination(address)).andReturn(false);
      ss.check(address, CheckType.CREATE, conn);
      expect(po.getBinding(address)).andReturn(null);
      expect(po.addBinding(address, address, null, false, true)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      conn.addTemporaryQueue(queue);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      session.createQueue(address, address, null, true, true);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateQueueWithFilter() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      final SimpleString filter = new SimpleString("myprop = 'ssf'");
      final Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.containsDestination(address)).andReturn(false);
      ss.check(address, CheckType.CREATE, conn);
      expect(po.getBinding(address)).andReturn(null);
      expect(po.addBinding(eq(address), eq(address), (Filter) anyObject(), eq(true), eq(false))).andAnswer(new IAnswer<Binding>()
      {
         public Binding answer() throws Throwable
         {
            assertEquals(filter, ((Filter) getCurrentArguments()[2]).getFilterString());
            return binding;
         }
      });
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      session.createQueue(address, address, filter, true, false);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testCreateQueue() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.containsDestination(address)).andReturn(false);
      ss.check(address, CheckType.CREATE, conn);
      expect(po.getBinding(address)).andReturn(null);
      expect(po.addBinding(address, address, null, true, false)).andReturn(binding);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
      session.createQueue(address, address, null, true, false);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding);
   }

   public void testDeleteQueue() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.removeBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(queue.getConsumerCount()).andReturn(0);
      expect(queue.isDurable()).andReturn(false);
      expect(queue.isTemporary()).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      session.deleteQueue(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testDeleteQueueTemporary() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.removeBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(queue.getConsumerCount()).andReturn(0);
      expect(queue.isDurable()).andReturn(false);
      expect(queue.isTemporary()).andReturn(true);
      conn.removeTemporaryQueue(queue);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      session.deleteQueue(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testDeleteQueueDurable() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.removeBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(queue.getConsumerCount()).andReturn(0);
      expect(queue.isDurable()).andReturn(true);
      queue.deleteAllReferences(sm);
      expect(queue.isTemporary()).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      session.deleteQueue(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testDeleteQueueWithConsumers() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.removeBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andStubReturn(queue);
      expect(queue.getConsumerCount()).andReturn(1);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      try
      {
         session.deleteQueue(address);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.ILLEGAL_STATE);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testDeleteQueueDoesntExist() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.removeBinding(address)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      try
      {
         session.deleteQueue(address);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(e.getCode(), MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testExecuteQueueQuery() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      expect(queue.getFilter()).andReturn(null);
      expect(queue.isDurable()).andReturn(true);
      expect(queue.isTemporary()).andReturn(true);
      expect(queue.getMaxSizeBytes()).andReturn(12345);
      expect(queue.getConsumerCount()).andReturn(2);
      expect(queue.getMessageCount()).andReturn(6789);
      expect(binding.getAddress()).andReturn(address);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(address);
      SessionQueueQueryResponseMessage resp = session.executeQueueQuery(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      assertEquals(resp.isDurable(),true);
      assertEquals(resp.isTemporary(),true);
      assertEquals(resp.isExists(),true);
      assertEquals(resp.getMaxSize(),12345);
      assertEquals(resp.getConsumerCount(),2);
      assertEquals(resp.getMessageCount(),6789);
      assertEquals(resp.getAddress(),address);
      assertEquals(resp.getFilterString(),null);
   }

   public void testExecuteQueueQueryWithFilter() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      Filter filter = createStrictMock(Filter.class);
      expect(po.getBinding(address)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      expect(queue.getFilter()).andReturn(filter);
      SimpleString filterString = new SimpleString("myprop = 'ddddd'");
      expect(filter.getFilterString()).andReturn(filterString);
      expect(queue.isDurable()).andReturn(true);
      expect(queue.isTemporary()).andReturn(true);
      expect(queue.getMaxSizeBytes()).andReturn(12345);
      expect(queue.getConsumerCount()).andReturn(2);
      expect(queue.getMessageCount()).andReturn(6789);
      expect(binding.getAddress()).andReturn(address);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, filter);
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(address);
      SessionQueueQueryResponseMessage resp = session.executeQueueQuery(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, filter);
      assertEquals(resp.isDurable(),true);
      assertEquals(resp.isTemporary(),true);
      assertEquals(resp.isExists(),true);
      assertEquals(resp.getMaxSize(),12345);
      assertEquals(resp.getConsumerCount(),2);
      assertEquals(resp.getMessageCount(),6789);
      assertEquals(resp.getAddress(),address);
      assertEquals(resp.getFilterString(),filterString);
   }

   public void testExecuteQueueQueryNoBinding() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      expect(po.getBinding(address)).andReturn(null);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(address);
      SessionQueueQueryResponseMessage resp = session.executeQueueQuery(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      assertEquals(resp.isExists(),false);
   }

   public void testExecuteQueueQueryNoQueueNameThrowsException() throws Exception
   {
      ServerSessionImpl session = create(false);
      Binding binding = createStrictMock(Binding.class);
      Queue queue = createStrictMock(Queue.class);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
      SessionQueueQueryMessage message = new SessionQueueQueryMessage();
      try
      {
         session.executeQueueQuery(null);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue);
   }

   public void testExecuteBindingQuery() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      Binding binding = createStrictMock(Binding.class);
      Binding binding2 = createStrictMock(Binding.class);
      List<Binding> bindings = new ArrayList<Binding>();
      bindings.add(binding);
      bindings.add(binding2);
      SimpleString qName = new SimpleString("q1");
      Queue queue = createStrictMock(Queue.class);
      SimpleString qName2 = new SimpleString("q2");
      Queue queue2 = createStrictMock(Queue.class);
      expect(po.containsDestination(address)).andReturn(true);
      expect(po.getBindingsForAddress(address)).andReturn(bindings);
      expect(binding.getQueue()).andReturn(queue);
      expect(binding2.getQueue()).andReturn(queue2);
      expect(queue.getName()).andReturn(qName);
      expect(queue2.getName()).andReturn(qName2);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, binding2, queue2);
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(address);
      SessionBindingQueryResponseMessage resp = session.executeBindingQuery(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, binding, queue, binding2, queue2);
      assertEquals(resp.isExists(),true);
      assertEquals(resp.getQueueNames().size(),2);
      assertEquals(resp.getQueueNames().get(0),qName);
      assertEquals(resp.getQueueNames().get(1),qName2);

   }

   public void testExecuteBindingQueryDoesntExist() throws Exception
   {
      ServerSessionImpl session = create(false);
      SimpleString address = new SimpleString("testQ");
      expect(po.containsDestination(address)).andReturn(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(address);
      SessionBindingQueryResponseMessage resp = session.executeBindingQuery(address);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
      assertEquals(resp.isExists(),false);
      assertEquals(resp.getQueueNames().size(),0);

   }

   public void testExecuteBindingQueryNoQName() throws Exception
   {
      ServerSessionImpl session = create(false);
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      SessionBindingQueryMessage message = new SessionBindingQueryMessage();
      try
      {
         session.executeBindingQuery(null);
         fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);

   }
   
   // Private -----------------------------------------------------------------------------------

   private ServerSessionImpl create(boolean xa)
           throws Exception
   {
      conn = createStrictMock(ServerConnection.class);
      returner = createStrictMock(RemotingConnection.class);
      sm = createStrictMock(StorageManager.class);
      po = createStrictMock(PostOffice.class);
      qs = createStrictMock(HierarchicalRepository.class);
      rm = createStrictMock(ResourceManager.class);
      ss = createStrictMock(SecurityStore.class);
      pd = createStrictMock(PacketDispatcher.class);
      executor = createStrictMock(Executor.class);
      expect(pd.generateID()).andReturn(1l);
      if (!xa)
      {
         expect(sm.generateTransactionID()).andReturn(999l);
      }
      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, xa,
              returner, sm, po, qs, rm, ss, pd, executor);
      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
      reset(conn, returner, sm, po, qs, rm, ss, pd, executor);
      return session;
   }


   private void testAcknowledgeAllUpToNotAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);

      final long txID = 918291;
      expect(sm.generateTransactionID()).andReturn(txID);

      final int numRefs = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = createStrictMock(MessageReference.class);

         expect(consumer.getClientTargetID()).andReturn(76767L);
         expect(ref.getMessage()).andReturn(createMock(ServerMessage.class));
         expect(ref.getDeliveryCount()).andReturn(0);
         returner.sendOneWay(isA(ReceiveMessage.class));
         refs.add(ref);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      replay(refs.toArray());

      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref : refs)
      {
         session.handleDelivery(ref, consumer);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      verify(refs.toArray());

      assertEquals(numRefs, session.getDeliveries().size());

      int i = 0;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack half of them

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      reset(refs.toArray());

      Queue queue = createMock(Queue.class);
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(durableMsg);
         if (durableMsg)
         {
            expect(queue.isDurable()).andStubReturn(durableQueue);
            if (durableQueue)
            {
               expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
               final long queueID = 1929012;
               expect(queue.getPersistenceID()).andReturn(queueID);
               expect(msg.getMessageID()).andStubReturn(i);
               sm.storeAcknowledgeTransactional(txID, queueID, i);
            }
         }
         replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs / 2)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      session.acknowledge(numRefs / 2 - 1, true);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(numRefs / 2, session.getDeliveries().size());

      i = numRefs / 2;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack the rest

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      reset(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(true);
         expect(queue.isDurable()).andStubReturn(true);
         expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         expect(queue.getPersistenceID()).andReturn(queueID);
         expect(msg.getMessageID()).andStubReturn(i);
         sm.storeAcknowledgeTransactional(txID, queueID, i);
         replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      session.acknowledge(numRefs - 1, true);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(0, session.getDeliveries().size());
   }

   private void testAcknowledgeNotAllUpToNotAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);

      final long txID = 918291;
      expect(sm.generateTransactionID()).andReturn(txID);

      final int numRefs = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = createStrictMock(MessageReference.class);

         expect(consumer.getClientTargetID()).andReturn(76767L);
         expect(ref.getMessage()).andReturn(createMock(ServerMessage.class));
         expect(ref.getDeliveryCount()).andReturn(0);
         returner.sendOneWay(isA(ReceiveMessage.class));
         refs.add(ref);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      replay(refs.toArray());

      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref : refs)
      {
         session.handleDelivery(ref, consumer);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      verify(refs.toArray());

      assertEquals(numRefs, session.getDeliveries().size());

      int i = 0;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack half of them

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      reset(refs.toArray());

      Queue queue = createMock(Queue.class);
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(durableMsg);
         if (durableMsg)
         {
            expect(queue.isDurable()).andStubReturn(durableQueue);
            if (durableQueue)
            {
               expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
               final long queueID = 1929012;
               expect(queue.getPersistenceID()).andReturn(queueID);
               expect(msg.getMessageID()).andStubReturn(i);
               sm.storeAcknowledgeTransactional(txID, queueID, i);
            }
         }
         replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs / 2)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());
      for (i = 0; i < numRefs / 2; i++)
      {
         session.acknowledge(i, false);
      }


      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(numRefs / 2, session.getDeliveries().size());

      i = numRefs / 2;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack the rest

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      reset(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(true);
         expect(queue.isDurable()).andStubReturn(true);
         expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         expect(queue.getPersistenceID()).andReturn(queueID);
         expect(msg.getMessageID()).andStubReturn(i);
         sm.storeAcknowledgeTransactional(txID, queueID, i);
         replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         session.acknowledge(i, false);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(0, session.getDeliveries().size());
   }

   private void testAcknowledgeAllUpToAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);
      expect(sm.generateTransactionID()).andReturn(71626L);

      final int numRefs = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = createStrictMock(MessageReference.class);

         expect(consumer.getClientTargetID()).andReturn(76767L);
         expect(ref.getMessage()).andReturn(createMock(ServerMessage.class));
         expect(ref.getDeliveryCount()).andReturn(0);
         returner.sendOneWay(isA(ReceiveMessage.class));
         refs.add(ref);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      replay(refs.toArray());

      ServerSessionImpl session = new ServerSessionImpl(conn, false, true, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref : refs)
      {
         session.handleDelivery(ref, consumer);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      verify(refs.toArray());

      assertEquals(numRefs, session.getDeliveries().size());

      int i = 0;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack half of them

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      reset(refs.toArray());

      Queue queue = createMock(Queue.class);
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(durableMsg);
         expect(queue.isDurable()).andStubReturn(durableQueue);
         if (durableMsg && durableQueue)
         {
            expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
            final long queueID = 1929012;
            expect(queue.getPersistenceID()).andReturn(queueID);
            expect(msg.getMessageID()).andStubReturn(i);
            sm.storeAcknowledge(queueID, i);
         }
         replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs / 2)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      session.acknowledge(numRefs / 2 - 1, true);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(numRefs / 2, session.getDeliveries().size());

      i = numRefs / 2;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack the rest

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      reset(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(true);
         expect(queue.isDurable()).andStubReturn(true);
         expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         expect(queue.getPersistenceID()).andReturn(queueID);
         expect(msg.getMessageID()).andStubReturn(i);
         sm.storeAcknowledge(queueID, i);
         replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      session.acknowledge(numRefs - 1, true);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(0, session.getDeliveries().size());
   }

   private void testAcknowledgeNotAllUpToAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerConsumer consumer = createStrictMock(ServerConsumer.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);
      expect(sm.generateTransactionID()).andReturn(71626L);

      final int numRefs = 10;

      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = createStrictMock(MessageReference.class);

         expect(consumer.getClientTargetID()).andReturn(76767L);
         expect(ref.getMessage()).andReturn(createMock(ServerMessage.class));
         expect(ref.getDeliveryCount()).andReturn(0);
         returner.sendOneWay(isA(ReceiveMessage.class));
         refs.add(ref);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      replay(refs.toArray());

      ServerSessionImpl session = new ServerSessionImpl(conn, false, true, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref : refs)
      {
         session.handleDelivery(ref, consumer);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      verify(refs.toArray());

      assertEquals(numRefs, session.getDeliveries().size());

      int i = 0;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack half of them

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      reset(refs.toArray());

      Queue queue = createMock(Queue.class);
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(durableMsg);
         expect(queue.isDurable()).andStubReturn(durableQueue);
         if (durableMsg && durableQueue)
         {
            expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
            final long queueID = 1929012;
            expect(queue.getPersistenceID()).andReturn(queueID);
            expect(msg.getMessageID()).andStubReturn(i);
            sm.storeAcknowledge(queueID, i);
         }
         replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs / 2)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      for (i = 0; i < numRefs / 2; i++)
      {
         session.acknowledge(i, false);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(numRefs / 2, session.getDeliveries().size());

      i = numRefs / 2;
      for (Delivery del : session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }

      //Now ack the rest

      reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      reset(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = createMock(ServerMessage.class);

         expect(ref.getMessage()).andStubReturn(msg);
         expect(ref.getQueue()).andStubReturn(queue);
         expect(msg.isDurable()).andStubReturn(true);
         expect(queue.isDurable()).andStubReturn(true);
         expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         expect(queue.getPersistenceID()).andReturn(queueID);
         expect(msg.getMessageID()).andStubReturn(i);
         sm.storeAcknowledge(queueID, i);
         replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs)
         {
            break;
         }
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      replay(refs.toArray());

      for (i = numRefs / 2; i < numRefs; i++)
      {
         session.acknowledge(i, false);
      }

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      verify(refs.toArray());

      assertEquals(0, session.getDeliveries().size());
   }

   private void testAutoCommitSend(final boolean persistent) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerMessage msg = createStrictMock(ServerMessage.class);
      MessageReference ref1 = createStrictMock(MessageReference.class);
      MessageReference ref2 = createStrictMock(MessageReference.class);
      MessageReference ref3 = createStrictMock(MessageReference.class);
      Queue queue1 = createStrictMock(Queue.class);
      Queue queue2 = createStrictMock(Queue.class);
      Queue queue3 = createStrictMock(Queue.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);
      expect(sm.generateTransactionID()).andReturn(71626L);

      final SimpleString dest = new SimpleString("blah");
      expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);

      final long messageID = 81129873;
      expect(sm.generateMessageID()).andReturn(messageID);
      msg.setMessageID(messageID);

      final long connectionID = 12734450;
      expect(conn.getID()).andReturn(connectionID);
      msg.setConnectionID(connectionID);

      List<MessageReference> refs = new ArrayList<MessageReference>();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      expect(po.route(msg)).andReturn(refs);
      expect(msg.getDurableRefCount()).andReturn(persistent ? 3 : 0);
      if (persistent)
      {
         sm.storeMessage(msg);
      }
      expect(ref1.getQueue()).andReturn(queue1);
      expect(ref2.getQueue()).andReturn(queue2);
      expect(ref3.getQueue()).andReturn(queue3);
      expect(queue1.addLast(ref1)).andReturn(HandleStatus.HANDLED);
      expect(queue2.addLast(ref2)).andReturn(HandleStatus.HANDLED);
      expect(queue3.addLast(ref3)).andReturn(HandleStatus.HANDLED);

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3, queue1, queue2, queue3);

      ServerSessionImpl session = new ServerSessionImpl(conn, true, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      session.send(msg);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3, queue1, queue2, queue3);
   }

   private void testNotAutoCommitSend(final boolean persistent) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);
      ServerMessage msg = createStrictMock(ServerMessage.class);
      MessageReference ref1 = createStrictMock(MessageReference.class);
      MessageReference ref2 = createStrictMock(MessageReference.class);
      MessageReference ref3 = createStrictMock(MessageReference.class);

      final long sessionID = 102981029;

      expect(pd.generateID()).andReturn(sessionID);

      final long transactionID = 27334;
      expect(sm.generateTransactionID()).andReturn(transactionID);

      final SimpleString dest = new SimpleString("blah");
      expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);

      final long messageID = 81129873;
      expect(sm.generateMessageID()).andReturn(messageID);
      msg.setMessageID(messageID);

      final long connectionID = 12734450;
      expect(conn.getID()).andReturn(connectionID);
      msg.setConnectionID(connectionID);

      List<MessageReference> refs = new ArrayList<MessageReference>();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      expect(po.route(msg)).andReturn(refs);
      expect(msg.getDurableRefCount()).andReturn(persistent ? 3 : 0);
      if (persistent)
      {
         sm.storeMessageTransactional(transactionID, msg);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3);

      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
              returner, sm, po, qs, rm, ss, pd, executor);

      session.send(msg);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3);
   }


   private void testConstructor(final boolean xa) throws Exception
   {
      ServerConnection conn = createStrictMock(ServerConnection.class);
      RemotingConnection returner = createStrictMock(RemotingConnection.class);
      StorageManager sm = createStrictMock(StorageManager.class);
      PostOffice po = createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = createStrictMock(ResourceManager.class);
      SecurityStore ss = createStrictMock(SecurityStore.class);
      PacketDispatcher pd = createStrictMock(PacketDispatcher.class);
      Executor executor = createStrictMock(Executor.class);

      final long id = 102981029;

      expect(pd.generateID()).andReturn(id);

      if (!xa)
      {
         expect(sm.generateTransactionID()).andReturn(71626L);
      }

      replay(conn, returner, sm, po, qs, rm, ss, pd, executor);

      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, xa,
              returner, sm, po, qs, rm, ss, pd, executor);

      verify(conn, returner, sm, po, qs, rm, ss, pd, executor);

      assertEquals(id, session.getID());
   }
}
