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
package org.jboss.messaging.tests.unit.core.persistence.impl.journal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.TestableJournal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A JournalStorageManagerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JournalStorageManagerTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(JournalStorageManagerTest.class);

   public void testStoreMessage() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
      long msgID = 1021092;
      EasyMock.expect(msg.getMessageID()).andReturn(msgID);
      messageJournal.appendAddRecord(msgID, JournalStorageManager.ADD_MESSAGE, msg);
      EasyMock.replay(messageJournal, bindingsJournal, msg);
      jsm.storeMessage(msg);
      EasyMock.verify(messageJournal, bindingsJournal, msg, msg);
   }

   public void testStoreAcknowledge() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long queueID = 1210981;
      final long messageID = 101921092;

      messageJournal.appendUpdateRecord(EasyMock.eq(messageID),
                                        EasyMock.eq(JournalStorageManager.ACKNOWLEDGE_REF),
                                        encodingMatch(autoEncode(queueID)));
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.storeAcknowledge(queueID, messageID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testStoreDelete() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long messageID = 101921092;

      messageJournal.appendDeleteRecord(messageID);
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.storeDelete(messageID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testStoreMessageTransactional() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
      long msgID = 1021092;
      EasyMock.expect(msg.getMessageID()).andReturn(msgID);
      long txID = 192872;
      messageJournal.appendAddRecordTransactional(txID, msgID, JournalStorageManager.ADD_MESSAGE, msg);
      EasyMock.replay(messageJournal, bindingsJournal, msg);
      jsm.storeMessageTransactional(txID, msg);
      EasyMock.verify(messageJournal, bindingsJournal, msg, msg);
   }

   public void testStoreAcknowledgeTransactional() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long queueID = 1210981;
      final long messageID = 101921092;

      final long txID = 12091921;
      messageJournal.appendUpdateRecordTransactional(EasyMock.eq(txID),
                                                     EasyMock.eq(messageID),
                                                     EasyMock.eq(JournalStorageManager.ACKNOWLEDGE_REF),
                                                     encodingMatch(autoEncode(queueID)));
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.storeAcknowledgeTransactional(txID, queueID, messageID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testStoreDeleteTransactional() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long messageID = 101921092;
      final long txID = 1209373;

      messageJournal.appendDeleteRecordTransactional(txID, messageID, null);
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.storeDeleteTransactional(txID, messageID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testPrepare() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long txID = 1209373;

      Xid xid = new XidImpl("branch".getBytes(), 1, "globalid".getBytes());

      messageJournal.appendPrepareRecord(EasyMock.eq(txID), EasyMock.isA(EncodingSupport.class));
      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.prepare(txID, xid);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testCommit() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long txID = 1209373;

      messageJournal.appendCommitRecord(txID);
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.commit(txID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testRollback() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long txID = 1209373;

      messageJournal.appendRollbackRecord(txID);
      EasyMock.replay(messageJournal, bindingsJournal);
      jsm.rollback(txID);
      EasyMock.verify(messageJournal, bindingsJournal);
   }

   public void testUpdateDeliveryCount() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      final long msgID = 120912901;
      final long queueID = 1283743;
      final int deliveryCount = 4757;

      MessageReference ref = EasyMock.createStrictMock(MessageReference.class);
      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(ref.getQueue()).andReturn(queue);
      EasyMock.expect(queue.getPersistenceID()).andReturn(queueID);
      EasyMock.expect(ref.getMessage()).andStubReturn(msg);
      EasyMock.expect(msg.getMessageID()).andStubReturn(msgID);
      EasyMock.expect(ref.getDeliveryCount()).andReturn(deliveryCount);

      messageJournal.appendUpdateRecord(EasyMock.eq(msgID),
                                        EasyMock.eq(JournalStorageManager.UPDATE_DELIVERY_COUNT),
                                        compareEncodingSupport(autoEncode(queueID, deliveryCount)));
      EasyMock.replay(messageJournal, bindingsJournal, ref, msg, queue);
      jsm.updateDeliveryCount(ref);
      EasyMock.verify(messageJournal, bindingsJournal, ref, msg, queue);
   }

   public void testLoadMessages() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      messageJournal.load((List<RecordInfo>)EasyMock.anyObject(), (List<PreparedTransactionInfo>)EasyMock.anyObject());

      List<RecordInfo> records = new ArrayList<RecordInfo>();

      /*
       * Two add messages
       * Three ack messages - two for msg1 and one for msg2
       * One update delivery count
       */
      final byte msg1Type = 12;
      final long msg1Expiration = 1209102912;
      final long msg1Timestamp = 129293746;
      final byte msg1Priority = 7;
      final byte[] msg1Bytes = RandomUtil.randomBytes(1000);
      final long msg1ID = 32748;
      ServerMessage msg1 = new ServerMessageImpl(msg1Type,
                                                 true,
                                                 msg1Expiration,
                                                 msg1Timestamp,
                                                 msg1Priority,
                                                 new ByteBufferWrapper(ByteBuffer.wrap(msg1Bytes)));
      msg1.setDestination(new SimpleString("qwuiuqwi"));
      msg1.setMessageID(msg1ID);
      msg1.putStringProperty(new SimpleString("prop1"), new SimpleString("wibble"));
      byte[] encode = new byte[msg1.getEncodeSize()];
      MessagingBuffer encodeBuffer = new ByteBufferWrapper(ByteBuffer.wrap(encode));
      msg1.encode(encodeBuffer);
      RecordInfo record1 = new RecordInfo(msg1ID, JournalStorageManager.ADD_MESSAGE, encode, false);

      final byte msg2Type = 3;
      final long msg2Expiration = 98448;
      final long msg2Timestamp = 1626999;
      final byte msg2Priority = 2;
      final byte[] msg2Bytes = RandomUtil.randomBytes(1000);
      final long msg2ID = 7446;
      ServerMessage msg2 = new ServerMessageImpl(msg2Type,
                                                 true,
                                                 msg2Expiration,
                                                 msg2Timestamp,
                                                 msg2Priority,
                                                 new ByteBufferWrapper(ByteBuffer.wrap(msg2Bytes)));
      msg2.setDestination(new SimpleString("qw12ihjwdijwqd"));
      msg2.setMessageID(msg2ID);
      msg2.putStringProperty(new SimpleString("prop2"), new SimpleString("wibble"));
      byte[] encode2 = new byte[msg2.getEncodeSize()];
      MessagingBuffer encodeBuffer2 = new ByteBufferWrapper(ByteBuffer.wrap(encode2));
      msg2.encode(encodeBuffer2);
      RecordInfo record2 = new RecordInfo(msg2ID, JournalStorageManager.ADD_MESSAGE, encode2, false);

      final long queue1ID = 1210981;
      final byte[] ack1Bytes = new byte[16];
      ByteBuffer bb1 = ByteBuffer.wrap(ack1Bytes);
      bb1.putLong(queue1ID);
      bb1.putLong(msg1ID);
      RecordInfo record3 = new RecordInfo(msg1ID, JournalStorageManager.ACKNOWLEDGE_REF, ack1Bytes, true);

      final long queue2ID = 112323;
      final byte[] ack2Bytes = new byte[16];
      ByteBuffer bb2 = ByteBuffer.wrap(ack2Bytes);
      bb2.putLong(queue2ID);
      bb2.putLong(msg1ID);
      RecordInfo record4 = new RecordInfo(msg1ID, JournalStorageManager.ACKNOWLEDGE_REF, ack2Bytes, true);

      final long queue3ID = 374764;
      final byte[] ack3Bytes = new byte[16];
      ByteBuffer bb3 = ByteBuffer.wrap(ack3Bytes);
      bb3.putLong(queue3ID);
      bb3.putLong(msg2ID);
      RecordInfo record5 = new RecordInfo(msg2ID, JournalStorageManager.ACKNOWLEDGE_REF, ack3Bytes, true);

      final int deliveryCount = 4757;
      byte[] updateBytes = new byte[12];
      ByteBuffer bb4 = ByteBuffer.wrap(updateBytes);
      bb4.putLong(queue1ID);
      bb4.putInt(deliveryCount);
      RecordInfo record6 = new RecordInfo(msg1ID, JournalStorageManager.UPDATE_DELIVERY_COUNT, updateBytes, true);

      records.add(record1);
      records.add(record2);
      records.add(record3);
      records.add(record4);
      records.add(record5);
      records.add(record6);

      EasyMock.expectLastCall().andAnswer(new LoadRecordsIAnswer(msg1ID, records, null));

      PostOffice po = EasyMock.createStrictMock(PostOffice.class);

      List<MessageReference> refs1 = new ArrayList<MessageReference>();
      MessageReference ref1_1 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref1_2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref1_3 = EasyMock.createStrictMock(MessageReference.class);
      refs1.add(ref1_1);
      refs1.add(ref1_2);
      refs1.add(ref1_3);
      EasyMock.expect(po.route(eqServerMessage(msg1))).andReturn(refs1);

      Queue queue1 = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);

      EasyMock.expect(ref1_1.getQueue()).andReturn(queue1);
      EasyMock.expect(ref1_2.getQueue()).andReturn(queue2);
      EasyMock.expect(ref1_3.getQueue()).andReturn(queue3);

      EasyMock.expect(queue1.addLast(ref1_1)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue2.addLast(ref1_2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue3.addLast(ref1_3)).andReturn(HandleStatus.HANDLED);

      List<MessageReference> refs2 = new ArrayList<MessageReference>();
      MessageReference ref2_1 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref2_2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref2_3 = EasyMock.createStrictMock(MessageReference.class);
      refs2.add(ref2_1);
      refs2.add(ref2_2);
      refs2.add(ref2_3);
      EasyMock.expect(po.route(eqServerMessage(msg2))).andReturn(refs2);

      EasyMock.expect(ref2_1.getQueue()).andReturn(queue1);
      EasyMock.expect(ref2_2.getQueue()).andReturn(queue2);
      EasyMock.expect(ref2_3.getQueue()).andReturn(queue3);

      EasyMock.expect(queue1.addLast(ref2_1)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue2.addLast(ref2_2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue3.addLast(ref2_3)).andReturn(HandleStatus.HANDLED);

      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      queues.put(queue1ID, queue1);
      queues.put(queue2ID, queue2);
      queues.put(queue3ID, queue3);

      EasyMock.expect(queue1.removeReferenceWithID(msg1ID)).andReturn(ref1_1);
      EasyMock.expect(queue2.removeReferenceWithID(msg1ID)).andReturn(ref1_2);
      EasyMock.expect(queue3.removeReferenceWithID(msg2ID)).andReturn(ref2_3);

      EasyMock.expect(queue1.getReference(msg1ID)).andReturn(ref1_1);
      ref1_1.setDeliveryCount(deliveryCount);

      EasyMock.replay(messageJournal, bindingsJournal, po);
      EasyMock.replay(refs1.toArray());
      EasyMock.replay(refs2.toArray());
      EasyMock.replay(queue1, queue2, queue3);

      jsm.loadMessages(po, queues, null);

      EasyMock.verify(messageJournal, bindingsJournal, po);
      EasyMock.verify(refs1.toArray());
      EasyMock.verify(refs2.toArray());
      EasyMock.verify(queue1, queue2, queue3);
   }

   public void testAddBindingWithFilter() throws Exception
   {
      testAddBindingWithFilter(true);
   }

   public void testAddBindingWithoutFilter() throws Exception
   {
      testAddBindingWithFilter(false);
   }

   private void testAddBindingWithFilter(final boolean useFilter) throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      Queue queue = EasyMock.createStrictMock(Queue.class);
      SimpleString queueName = new SimpleString("saiohsiudh");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      Filter filter = null;
      SimpleString queueFilter = new SimpleString("ihjuhyg");
      if (useFilter)
      {
         filter = EasyMock.createStrictMock(Filter.class);
         EasyMock.expect(filter.getFilterString()).andStubReturn(queueFilter);
      }
      EasyMock.expect(queue.getFilter()).andStubReturn(filter);

      SimpleString address = new SimpleString("aijsiajs");
      Binding binding = new BindingImpl(address, queue);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);

      queue.setPersistenceID(0);

      byte[] nameBytes = queueName.getData();
      daos.writeInt(nameBytes.length);
      daos.write(nameBytes);
      byte[] addressBytes = binding.getAddress().getData();
      daos.writeInt(addressBytes.length);
      daos.write(addressBytes);
      daos.writeBoolean(filter != null);
      if (filter != null)
      {
         byte[] filterBytes = queueFilter.getData();
         daos.writeInt(filterBytes.length);
         daos.write(filterBytes);
      }
      daos.flush();
      byte[] data = baos.toByteArray();

      log.debug("** data length is " + data.length);
      log.debug(UnitTestCase.dumpBytes(data));

      bindingsJournal.appendAddRecord(EasyMock.eq(0L),
                                      EasyMock.eq(JournalStorageManager.BINDING_RECORD),
                                      compareEncodingSupport(data));

      if (useFilter)
      {
         EasyMock.replay(queue, filter, messageJournal, bindingsJournal);
      }
      else
      {
         EasyMock.replay(queue, messageJournal, bindingsJournal);
      }

      jsm.addBinding(binding);

      if (useFilter)
      {
         EasyMock.verify(queue, filter, messageJournal, bindingsJournal);
      }
      else
      {
         EasyMock.verify(queue, messageJournal, bindingsJournal);
      }
   }

   public void testDeleteBinding() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      Binding binding = EasyMock.createStrictMock(Binding.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      long queueID = 1209129;
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(queueID);

      bindingsJournal.appendDeleteRecord(queueID);

      EasyMock.replay(messageJournal, bindingsJournal, binding, queue);

      jsm.deleteBinding(binding);

      EasyMock.verify(messageJournal, bindingsJournal, binding, queue);
   }

   public void testDeleteBindingUnPersistedQueue() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      Binding binding = EasyMock.createStrictMock(Binding.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      long queueID = -1;
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(queueID);

      EasyMock.replay(messageJournal, bindingsJournal, binding, queue);

      try
      {
         jsm.deleteBinding(binding);
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         // OK
      }

      EasyMock.verify(messageJournal, bindingsJournal, binding, queue);
   }

   public void testAddDeleteDestination() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      SimpleString dest = new SimpleString("oaskokas");

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);
      byte[] destBytes = dest.getData();
      daos.writeInt(destBytes.length);
      daos.write(destBytes);
      daos.flush();
      byte[] data = baos.toByteArray();
      bindingsJournal.appendAddRecord(EasyMock.eq(0L),
                                      EasyMock.eq(JournalStorageManager.DESTINATION_RECORD),
                                      compareEncodingSupport(data));

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.addDestination(dest);

      EasyMock.verify(messageJournal, bindingsJournal);

      EasyMock.reset(messageJournal, bindingsJournal);

      // Adding again should do nothing

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.addDestination(dest);

      EasyMock.verify(messageJournal, bindingsJournal);

      EasyMock.reset(messageJournal, bindingsJournal);

      // Add diffferent dest

      SimpleString dest2 = new SimpleString("ihjij");

      baos = new ByteArrayOutputStream();
      daos = new DataOutputStream(baos);
      destBytes = dest2.getData();
      daos.writeInt(destBytes.length);
      daos.write(destBytes);
      daos.flush();
      data = baos.toByteArray();
      bindingsJournal.appendAddRecord(EasyMock.eq(2L),
                                      EasyMock.eq(JournalStorageManager.DESTINATION_RECORD),
                                      compareEncodingSupport(data));

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.addDestination(dest2);

      EasyMock.verify(messageJournal, bindingsJournal);

      EasyMock.reset(messageJournal, bindingsJournal);

      bindingsJournal.appendDeleteRecord(2);

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.deleteDestination(dest2);

      EasyMock.verify(messageJournal, bindingsJournal);

      EasyMock.reset(messageJournal, bindingsJournal);

      EasyMock.replay(messageJournal, bindingsJournal);

      // Should do nothing

      jsm.deleteDestination(dest2);

      EasyMock.verify(messageJournal, bindingsJournal);
   }

   private RecordInfo createBindingRecord(final long id,
                                          final SimpleString queueName,
                                          final SimpleString address,
                                          final SimpleString filter) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);

      byte[] nameBytes = queueName.getData();
      daos.writeInt(nameBytes.length);
      daos.write(nameBytes);
      byte[] addressBytes = address.getData();
      daos.writeInt(addressBytes.length);
      daos.write(addressBytes);
      daos.writeBoolean(filter != null);
      if (filter != null)
      {
         byte[] filterBytes = filter.getData();
         daos.writeInt(filterBytes.length);
         daos.write(filterBytes);
      }
      daos.flush();
      byte[] data = baos.toByteArray();

      RecordInfo record = new RecordInfo(id, JournalStorageManager.BINDING_RECORD, data, false);

      return record;
   }

   private RecordInfo createDestinationRecord(final long id, final SimpleString dest) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream daos = new DataOutputStream(baos);
      byte[] destBytes = dest.getData();
      daos.writeInt(destBytes.length);
      daos.write(destBytes);
      daos.flush();
      byte[] data = baos.toByteArray();

      RecordInfo record = new RecordInfo(id, JournalStorageManager.DESTINATION_RECORD, data, false);

      return record;
   }

   public void testLoadBindings() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      bindingsJournal.load((List<RecordInfo>)EasyMock.anyObject(), (List<PreparedTransactionInfo>)EasyMock.anyObject());

      List<RecordInfo> records = new ArrayList<RecordInfo>();

      SimpleString squeue1 = new SimpleString("queue1");
      SimpleString squeue2 = new SimpleString("queue2");
      SimpleString squeue3 = new SimpleString("queue3");
      SimpleString saddress1 = new SimpleString("address1");
      SimpleString saddress2 = new SimpleString("address2");
      SimpleString saddress3 = new SimpleString("address3");
      SimpleString sfilter1 = new SimpleString("JBMMessageID=123");

      records.add(createBindingRecord(0, squeue1, saddress1, sfilter1));
      records.add(createBindingRecord(1, squeue2, saddress2, null));
      records.add(createBindingRecord(2, squeue3, saddress3, null));

      SimpleString sdest1 = new SimpleString("dest1");
      SimpleString sdest2 = new SimpleString("dest2");
      SimpleString sdest3 = new SimpleString("dest3");

      records.add(createDestinationRecord(10, sdest1));
      records.add(createDestinationRecord(11, sdest2));
      records.add(createDestinationRecord(12, sdest3));

      EasyMock.expectLastCall().andAnswer(new LoadRecordsIAnswer(12, records, null));

      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);

      Queue queue1 = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      EasyMock.expect(qf.createQueue(EasyMock.eq(0L),
                                     EasyMock.eq(squeue1),
                                     EasyMock.isA(Filter.class),
                                     EasyMock.eq(true),
                                     EasyMock.eq(false))).andReturn(queue1);
      EasyMock.expect(qf.createQueue(1L, squeue2, null, true, false)).andReturn(queue1);
      EasyMock.expect(qf.createQueue(2L, squeue3, null, true, false)).andReturn(queue1);

      EasyMock.replay(messageJournal, bindingsJournal, queue1, queue2, queue3, qf);

      List<Binding> bindings = new ArrayList<Binding>();
      List<SimpleString> destinations = new ArrayList<SimpleString>();

      jsm.loadBindings(qf, bindings, destinations);

      EasyMock.verify(messageJournal, bindingsJournal, queue1, queue2, queue3, qf);

      assertEquals(3, bindings.size());
      assertEquals(3, destinations.size());

      assertEquals(saddress1, bindings.get(0).getAddress());
      assertEquals(saddress2, bindings.get(1).getAddress());
      assertEquals(saddress3, bindings.get(2).getAddress());

      assertEquals(sdest1, destinations.get(0));
      assertEquals(sdest2, destinations.get(1));
      assertEquals(sdest3, destinations.get(2));
   }

   public void testStartStop() throws Exception
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      assertFalse(jsm.isStarted());
      bindingsJournal.start();
      messageJournal.start();

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.start();

      assertTrue(jsm.isStarted());

      EasyMock.verify(messageJournal, bindingsJournal);

      EasyMock.reset(messageJournal, bindingsJournal);

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.start();

      EasyMock.verify(messageJournal, bindingsJournal);

      assertTrue(jsm.isStarted());

      EasyMock.reset(messageJournal, bindingsJournal);

      bindingsJournal.stop();

      messageJournal.stop();

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.stop();

      EasyMock.verify(messageJournal, bindingsJournal);

      assertFalse(jsm.isStarted());

      EasyMock.reset(messageJournal, bindingsJournal);

      EasyMock.replay(messageJournal, bindingsJournal);

      jsm.stop();

      EasyMock.verify(messageJournal, bindingsJournal);

      assertFalse(jsm.isStarted());
   }

   public void testGenerateMessageID()
   {
      Journal messageJournal = EasyMock.createStrictMock(Journal.class);
      Journal bindingsJournal = EasyMock.createStrictMock(Journal.class);

      JournalStorageManager jsm = new JournalStorageManager(messageJournal, bindingsJournal);

      long id = jsm.generateUniqueID();

      assertEquals(++id, jsm.generateUniqueID());
      assertEquals(++id, jsm.generateUniqueID());
      assertEquals(++id, jsm.generateUniqueID());
      assertEquals(++id, jsm.generateUniqueID());
      assertEquals(++id, jsm.generateUniqueID());
   }

   public void testConstructor()
   {
      Configuration config = new ConfigurationImpl();

      JournalStorageManager jsm = new JournalStorageManager(config);

      assertNotNull(jsm.getMessageJournal());

      TestableJournal messageJournal = (TestableJournal)jsm.getMessageJournal();

      assertEquals(config.getJournalFileSize(), messageJournal.getFileSize());
      assertEquals(config.getJournalMinFiles(), messageJournal.getMinFiles());
      assertEquals(config.isJournalSyncTransactional(), messageJournal.isSyncTransactional());
      assertEquals(config.isJournalSyncNonTransactional(), messageJournal.isSyncNonTransactional());
      assertEquals("jbm-data", messageJournal.getFilePrefix());
      assertEquals("jbm", messageJournal.getFileExtension());
      assertEquals(config.getJournalMaxAIO(), messageJournal.getMaxAIO());

      assertNotNull(jsm.getBindingsJournal());

      TestableJournal bindingsJournal = (TestableJournal)jsm.getBindingsJournal();

      assertEquals(1024 * 1024, bindingsJournal.getFileSize());
      assertEquals(2, bindingsJournal.getMinFiles());
      assertEquals(true, bindingsJournal.isSyncTransactional());
      assertEquals(true, bindingsJournal.isSyncNonTransactional());
      assertEquals("jbm-bindings", bindingsJournal.getFilePrefix());
      assertEquals("bindings", bindingsJournal.getFileExtension());
      assertEquals(1, bindingsJournal.getMaxAIO());
   }

   private EncodingSupport encodingMatch(final byte expectedRecord[])
   {

      EasyMock.reportMatcher(new IArgumentMatcher()
      {

         public void appendTo(final StringBuffer buffer)
         {
         }

         public boolean matches(final Object argument)
         {
            EncodingSupport support = (EncodingSupport)argument;

            if (support.getEncodeSize() != expectedRecord.length)
            {
               return false;
            }

            byte newByte[] = new byte[expectedRecord.length];

            ByteBuffer buffer = ByteBuffer.wrap(newByte);

            ByteBufferWrapper wrapper = new ByteBufferWrapper(buffer);

            support.encode(wrapper);

            byte encodingBytes[] = wrapper.array();

            for (int i = 0; i < encodingBytes.length; i++)
            {
               if (encodingBytes[i] != expectedRecord[i])
               {
                  return false;
               }
            }

            return true;
         }

      });

      return null;
   }

   public static ServerMessage eqServerMessage(final ServerMessage serverMessage)
   {
      EasyMock.reportMatcher(new ServerMessageMatcher(serverMessage));
      return serverMessage;
   }

   static class ServerMessageMatcher implements IArgumentMatcher
   {
      ServerMessage msg;

      public ServerMessageMatcher(final ServerMessage msg)
      {
         this.msg = msg;
      }

      public boolean matches(final Object o)
      {
         ServerMessage that = (ServerMessage)o;

         boolean matches = msg.getMessageID() == that.getMessageID() && msg.isDurable() == that.isDurable() &&
                           msg.getType() == that.getType() &&
                           msg.getTimestamp() == that.getTimestamp() &&
                           msg.getExpiration() == that.getExpiration() &&
                           msg.getPriority() == that.getPriority() &&
                           msg.getDestination().equals(that.getDestination());

         if (matches)
         {
            byte[] bod1 = msg.getBody().array();
            byte[] bod2 = that.getBody().array();
            if (bod1.length == bod2.length)
            {
               for (int i = 0; i < bod1.length; i++)
               {
                  if (bod1[i] != bod2[i])
                  {
                     return false;
                  }
               }
               return true;
            }
         }

         return matches;
      }

      public void appendTo(final StringBuffer stringBuffer)
      {
         stringBuffer.append("Aunty Mabel knits great socks");
      }
   }

   class LoadRecordsIAnswer implements IAnswer
   {
      long maxID;

      List<RecordInfo> records;

      List<PreparedTransactionInfo> preparedTransactions;

      public LoadRecordsIAnswer(final long maxID,
                                final List<RecordInfo> records,
                                final List<PreparedTransactionInfo> preparedTransactions)
      {
         this.maxID = maxID;
         this.records = records;
         this.preparedTransactions = preparedTransactions;
      }

      public Object answer() throws Throwable
      {
         if (records != null)
         {
            List<RecordInfo> theRecords = (List<RecordInfo>)EasyMock.getCurrentArguments()[0];
            theRecords.addAll(records);
         }
         if (preparedTransactions != null)
         {
            List<PreparedTransactionInfo> thePreparedTxs = (List<PreparedTransactionInfo>)EasyMock.getCurrentArguments()[1];
            thePreparedTxs.addAll(preparedTransactions);
         }
         return maxID;
      }
   }
}
