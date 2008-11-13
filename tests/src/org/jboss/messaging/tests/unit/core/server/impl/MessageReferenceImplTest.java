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

import org.easymock.EasyMock;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.MessageReferenceImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessageReferenceImplTest extends UnitTestCase
{
   public void testDeliveryCount()
   {
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      assertEquals(messageReference.getDeliveryCount(), 0);
      messageReference.incrementDeliveryCount();
      messageReference.incrementDeliveryCount();
      messageReference.incrementDeliveryCount();
      messageReference.incrementDeliveryCount();
      messageReference.incrementDeliveryCount();
      assertEquals(messageReference.getDeliveryCount(), 5);
      messageReference.setDeliveryCount(0);
      assertEquals(messageReference.getDeliveryCount(), 0);
   }

   public void testCopy()
   {
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(999);
      messageReference.setScheduledDeliveryTime(System.currentTimeMillis());
      MessageReference messageReferenceCopy = messageReference.copy(queue2);
      assertEquals(messageReferenceCopy.getDeliveryCount(), messageReference.getDeliveryCount());
      assertEquals(messageReferenceCopy.getScheduledDeliveryTime(), messageReference.getScheduledDeliveryTime());
      assertEquals(messageReferenceCopy.getMessage(), messageReference.getMessage());
      assertEquals(messageReferenceCopy.getQueue(), queue2);
   }

   public void testCancelDurable() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      SimpleString queueName = new SimpleString("queueName");
      queue.referenceCancelled();
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      sm.updateDeliveryCount(messageReference);
      EasyMock.replay(sm, po, repos, serverMessage, queue);
      assertTrue(messageReference.cancel(sm, po, repos));
      EasyMock.verify(sm, po, repos, serverMessage, queue);
   }

   public void testCancelNonDurable() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      SimpleString queueName = new SimpleString("queueName");
      queue.referenceCancelled();
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(false);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      EasyMock.expect(queue.isDurable()).andStubReturn(false);
      EasyMock.replay(sm, po, repos, serverMessage, queue);
      assertTrue(messageReference.cancel(sm, po, repos));
      EasyMock.verify(sm, po, repos, serverMessage, queue);
   }

   public void testCancelToDLQExists() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setMaxDeliveryAttempts(1);
      SimpleString dlqName = new SimpleString("testDLQ");
      queueSettings.setDLQ(dlqName);
      
      Binding dlqBinding = EasyMock.createStrictMock(Binding.class);
      EasyMock.expect(dlqBinding.getAddress()).andReturn(dlqName);

      StorageManager sm = EasyMock.createNiceMock(StorageManager.class);
      
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(po.getPagingManager()).andStubReturn(pm);
      
      
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      
      ServerMessage serverMessage = EasyMock.createNiceMock(ServerMessage.class);
      
      Queue queue = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);
      
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(1);
      
      SimpleString queueName = new SimpleString("queueName");
      
      queue.referenceAcknowledged(messageReference);
      
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      
      sm.updateDeliveryCount(messageReference);
      
      EasyMock.expect(po.getBinding(dlqName)).andReturn(dlqBinding);
      
      EasyMock.expect(serverMessage.copy()).andReturn(serverMessage);
      
      EasyMock.expect(sm.generateUniqueID()).andReturn(2l);
      
      //serverMessage.setMessageID(2);
      
      EasyMock.expect(serverMessage.getDestination()).andReturn(queueName);
      
      serverMessage.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, queueName);
      serverMessage.setExpiration(0);
      serverMessage.setDestination(dlqName);
      
      EasyMock.expect(po.route(serverMessage)).andReturn(new ArrayList<MessageReference>());
      
      EasyMock.expect(serverMessage.getDurableRefCount()).andReturn(0);
      EasyMock.expect(serverMessage.decrementRefCount()).andReturn(1);
      EasyMock.expect(serverMessage.decrementDurableRefCount()).andReturn(0);
      
      EasyMock.expect(sm.generateUniqueID()).andReturn(1l);
      
      EasyMock.replay(sm, po, repos, serverMessage, queue, dlqBinding, pm);
      
      
      assertFalse(messageReference.cancel(sm, po, repos));
      EasyMock.verify(sm, po, repos, serverMessage, queue, dlqBinding, pm);
   }

   public void testCancelToDLQDoesntExist() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setMaxDeliveryAttempts(1);

      SimpleString dlqName = new SimpleString("testDLQ");
      
      queueSettings.setDLQ(dlqName);
      
      Binding dlqBinding = EasyMock.createStrictMock(Binding.class);
      
      EasyMock.expect(dlqBinding.getAddress()).andReturn(dlqName);
      
      StorageManager sm = EasyMock.createNiceMock(StorageManager.class);
      
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(po.getPagingManager()).andStubReturn(pm);
      EasyMock.expect(pm.isPaging(EasyMock.isA(SimpleString.class))).andStubReturn(false);
      pm.messageDone(EasyMock.isA(ServerMessage.class));
      EasyMock.expectLastCall().anyTimes();
      
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      
      ServerMessage serverMessage = EasyMock.createNiceMock(ServerMessage.class);
      
      Queue queue = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(1);
      
      SimpleString queueName = new SimpleString("queueName");
      
      queue.referenceAcknowledged(messageReference);
      
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      
      sm.updateDeliveryCount(messageReference);
      
      EasyMock.expect(po.getBinding(dlqName)).andReturn(null);
      EasyMock.expect(po.addBinding(dlqName, dlqName, null, true, false, false)).andReturn(dlqBinding);
      
      EasyMock.expect(serverMessage.copy()).andReturn(serverMessage);
      
      EasyMock.expect(sm.generateUniqueID()).andReturn(2l);
      
     // serverMessage.setMessageID(2);
      
      EasyMock.expect(serverMessage.getDestination()).andReturn(queueName);
      serverMessage.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, queueName);
      serverMessage.setExpiration(0);
      serverMessage.setDestination(dlqName);
      
      EasyMock.expect(po.route(serverMessage)).andReturn(new ArrayList<MessageReference>());
      
      EasyMock.expect(serverMessage.getDurableRefCount()).andReturn(0);
      EasyMock.expect(serverMessage.decrementDurableRefCount()).andReturn(0);
      
      EasyMock.expect(sm.generateUniqueID()).andReturn(1l);
      
      EasyMock.replay(sm, po, repos, serverMessage, queue, dlqBinding, pm);
      
      assertFalse(messageReference.cancel(sm, po, repos));
      
      EasyMock.verify(sm, po, repos, serverMessage, queue, dlqBinding, pm);
   }

   public void testExpire() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setMaxDeliveryAttempts(1);
      SimpleString dlqName = new SimpleString("testDLQ");
      queueSettings.setDLQ(dlqName);
      StorageManager sm = EasyMock.createNiceMock(StorageManager.class);
      
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(po.getPagingManager()).andStubReturn(pm);
      EasyMock.expect(pm.isPaging(EasyMock.isA(SimpleString.class))).andStubReturn(false);
      pm.messageDone(EasyMock.isA(ServerMessage.class));
      EasyMock.expectLastCall().anyTimes();

      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(1);
      SimpleString queueName = new SimpleString("queueName");
      queue.referenceAcknowledged(messageReference);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      EasyMock.expect(serverMessage.decrementRefCount()).andReturn(1);
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.decrementDurableRefCount()).andReturn(0);
      EasyMock.expect(sm.generateUniqueID()).andReturn(1l);

      EasyMock.replay(sm, po, repos, serverMessage, queue, pm);
      messageReference.expire(sm, po, repos);
      EasyMock.verify(sm, po, repos, serverMessage, queue, pm);
   }

   public void testExpireToQExists() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setMaxDeliveryAttempts(1);
      SimpleString expQName = new SimpleString("testexpQ");
      Binding expQBinding = EasyMock.createStrictMock(Binding.class);
      queueSettings.setExpiryQueue(expQName);
      StorageManager sm = EasyMock.createNiceMock(StorageManager.class);

      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(po.getPagingManager()).andStubReturn(pm);
      EasyMock.expect(pm.isPaging(EasyMock.isA(SimpleString.class))).andStubReturn(false);
      pm.messageDone(EasyMock.isA(ServerMessage.class));
      EasyMock.expectLastCall().anyTimes();
      
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      
      ServerMessage serverMessage = EasyMock.createNiceMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(1);
      SimpleString queueName = new SimpleString("queueName");
      queue.referenceAcknowledged(messageReference);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      EasyMock.expect(sm.generateUniqueID()).andReturn(2l);
      EasyMock.expect(sm.generateUniqueID()).andReturn(1l);
      EasyMock.expect(po.getBinding(expQName)).andReturn(expQBinding);
      EasyMock.expect(serverMessage.copy()).andReturn(serverMessage);
      //serverMessage.setMessageID(2);
      EasyMock.expect(serverMessage.getDestination()).andReturn(queueName);
      serverMessage.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, queueName);
      serverMessage.setExpiration(0);
      serverMessage.putLongProperty(EasyMock.eq(MessageImpl.HDR_ACTUAL_EXPIRY_TIME), EasyMock.anyLong());
      EasyMock.expect(expQBinding.getAddress()).andStubReturn(expQName);
      serverMessage.setDestination(expQName);
      EasyMock.expect(po.route(serverMessage)).andReturn(new ArrayList<MessageReference>());
      EasyMock.expect(serverMessage.getDurableRefCount()).andReturn(0);
      EasyMock.expect(serverMessage.decrementDurableRefCount()).andReturn(0);

      EasyMock.replay(sm, po, repos, serverMessage, queue, expQBinding, pm);
      
      messageReference.expire(sm, po, repos);

      EasyMock.verify(sm, po, repos, serverMessage, queue, expQBinding, pm);
   }

   public void testExpireToQDoesntExist() throws Exception
   {
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setMaxDeliveryAttempts(1);
      SimpleString expQName = new SimpleString("testexpQ");
      Binding expQBinding = EasyMock.createStrictMock(Binding.class);
      queueSettings.setExpiryQueue(expQName);
      StorageManager sm = EasyMock.createNiceMock(StorageManager.class);
      
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(po.getPagingManager()).andStubReturn(pm);
      EasyMock.expect(pm.isPaging(EasyMock.isA(SimpleString.class))).andStubReturn(false);
      pm.messageDone(EasyMock.isA(ServerMessage.class));
      EasyMock.expectLastCall().anyTimes();
      
      HierarchicalRepository<QueueSettings> repos = EasyMock.createStrictMock(HierarchicalRepository.class);
      
      ServerMessage serverMessage = EasyMock.createNiceMock(ServerMessage.class);
      
      Queue queue = EasyMock.createStrictMock(Queue.class);
      
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      messageReference.setDeliveryCount(1);
      SimpleString queueName = new SimpleString("queueName");
      queue.referenceAcknowledged(messageReference);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(repos.getMatch(queueName.toString())).andStubReturn(queueSettings);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(true);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(999l);
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      EasyMock.expect(sm.generateUniqueID()).andReturn(2l);
      EasyMock.expect(sm.generateUniqueID()).andReturn(1l);
      EasyMock.expect(po.getBinding(expQName)).andReturn(null);
      EasyMock.expect(po.addBinding(expQName, expQName, null, true, false, false)).andReturn(expQBinding);
      EasyMock.expect(serverMessage.copy()).andReturn(serverMessage);
     // serverMessage.setMessageID(2);
      EasyMock.expect(serverMessage.getDestination()).andReturn(queueName);
      serverMessage.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, queueName);
      serverMessage.setExpiration(0);
      serverMessage.putLongProperty(EasyMock.eq(MessageImpl.HDR_ACTUAL_EXPIRY_TIME), EasyMock.anyLong());
      EasyMock.expect(expQBinding.getAddress()).andStubReturn(expQName);
      serverMessage.setDestination(expQName);
      EasyMock.expect(po.route(serverMessage)).andReturn(new ArrayList<MessageReference>());
      EasyMock.expect(serverMessage.getDurableRefCount()).andReturn(0);
      EasyMock.expect(serverMessage.decrementDurableRefCount()).andReturn(0);

      EasyMock.replay(sm, po, repos, serverMessage, queue, expQBinding, pm);
      
      messageReference.expire(sm, po, repos);
      
      EasyMock.verify(sm, po, repos, serverMessage, queue, expQBinding, pm);
   }

   public void testMove() throws Exception
   {
      SimpleString fromAddress = RandomUtil.randomSimpleString();
      SimpleString toAddress = RandomUtil.randomSimpleString();
      long tid = RandomUtil.randomLong();
      long messageID = RandomUtil.randomLong();
      long newMessageID = RandomUtil.randomLong();
      
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Binding toBinding = EasyMock.createStrictMock(Binding.class);
      Queue toQueue = EasyMock.createStrictMock(Queue.class);
      PostOffice postOffice = EasyMock.createMock(PostOffice.class);
      
      
      PagingManager pm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pm.page(EasyMock.isA(ServerMessage.class))).andStubReturn(false);
      EasyMock.expect(postOffice.getPagingManager()).andStubReturn(pm);
      EasyMock.expect(pm.isPaging(EasyMock.isA(SimpleString.class))).andStubReturn(false);
      pm.messageDone(EasyMock.isA(ServerMessage.class));
      EasyMock.expectLastCall().anyTimes();
      
      StorageManager persistenceManager = EasyMock.createMock(StorageManager.class);
      ServerMessage serverMessage = EasyMock.createNiceMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getMessageID()).andStubReturn(1l);
      MessageReferenceImpl messageReference = new DummyMessageReference(serverMessage, queue);
      ServerMessage copyMessage = EasyMock.createNiceMock(ServerMessage.class);
      EasyMock.expect(copyMessage.getMessageID()).andStubReturn(1l);

      EasyMock.expect(persistenceManager.generateUniqueID()).andReturn(tid);
      EasyMock.expect(serverMessage.copy()).andReturn(copyMessage);
      EasyMock.expect(persistenceManager.generateUniqueID()).andReturn(newMessageID);
      copyMessage.setMessageID(newMessageID);
      EasyMock.expect(copyMessage.getDestination()).andReturn(fromAddress);
      copyMessage.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, fromAddress);
      copyMessage.setExpiration(0);
      EasyMock.expect(toBinding.getAddress()).andStubReturn(toAddress);
      copyMessage.setDestination(toAddress);
      EasyMock.expect(postOffice.route(copyMessage)).andReturn(new ArrayList<MessageReference>());
      EasyMock.expect(copyMessage.getDurableRefCount()).andReturn(0);
      EasyMock.expect(serverMessage.isDurable()).andStubReturn(false);
      EasyMock.expect(serverMessage.getMessageID()).andReturn(messageID);
      queue.referenceAcknowledged(messageReference);

      EasyMock.replay(queue, toBinding, toQueue, postOffice, persistenceManager, serverMessage, copyMessage, pm);
      
      messageReference.move(toBinding, persistenceManager, postOffice);
      
      EasyMock.verify(queue, toBinding, toQueue, postOffice, persistenceManager, serverMessage, copyMessage, pm);
   }
   
   //we need to override the constructor for creation
   class DummyMessageReference extends MessageReferenceImpl
   {
      protected DummyMessageReference(ServerMessage message, Queue queue)
      {
         super(message, queue);
      }
   }
}
