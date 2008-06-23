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
import java.util.concurrent.Executor;

import org.easymock.EasyMock;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.Delivery;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerSessionImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ServerSessionImplTest
 * 
 * Think of the things you can do with a session
 * 
 * test open close
 * test start stop
 * test constructor
 * test send some messages - both tx and not
 * 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerSessionImplTest extends UnitTestCase
{
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
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
                 
      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      EasyMock.expect(sm.generateTransactionID()).andReturn(71626L);
      
      final SimpleString dest = new SimpleString("blah");
      EasyMock.expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      EasyMock.expectLastCall().andThrow(new MessagingException(MessagingException.SECURITY_EXCEPTION));
            
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);
                  
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
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);          
   }
   
   public void testNotAutoCommitSendFailsSecurity() throws Exception
   {
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
                 
      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      EasyMock.expect(sm.generateTransactionID()).andReturn(71626L);
      
      final SimpleString dest = new SimpleString("blah");
      EasyMock.expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      EasyMock.expectLastCall().andThrow(new MessagingException(MessagingException.SECURITY_EXCEPTION));
            
                  
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg);
                  
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
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg); 
      
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
   
   // Private -----------------------------------------------------------------------------------
   
   private void testAcknowledgeAllUpToNotAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerConsumer consumer = EasyMock.createStrictMock(ServerConsumer.class);
      
      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      
      final long txID = 918291;
      EasyMock.expect(sm.generateTransactionID()).andReturn(txID);
      
      final int numRefs = 10;
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = EasyMock.createStrictMock(MessageReference.class);
         
         EasyMock.expect(consumer.getClientTargetID()).andReturn(76767L);
         EasyMock.expect(ref.getMessage()).andReturn(EasyMock.createMock(ServerMessage.class));
         EasyMock.expect(ref.getDeliveryCount()).andReturn(0);
         returner.send(EasyMock.isA(ReceiveMessage.class));
         refs.add(ref);
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.replay(refs.toArray());
      
      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
            returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref: refs)
      {
         session.handleDelivery(ref, consumer);
      }
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.verify(refs.toArray());
      
      assertEquals(numRefs, session.getDeliveries().size());
      
      int i = 0;
      for (Delivery del: session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }
      
      //Now ack half of them
      
      EasyMock.reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.reset(refs.toArray());
      
      Queue queue = EasyMock.createMock(Queue.class); 
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = EasyMock.createMock(ServerMessage.class);
                 
         EasyMock.expect(ref.getMessage()).andStubReturn(msg);
         EasyMock.expect(ref.getQueue()).andStubReturn(queue);
         EasyMock.expect(msg.isDurable()).andStubReturn(durableMsg);
         if (durableMsg)
         {
            EasyMock.expect(queue.isDurable()).andStubReturn(durableQueue);
            if (durableQueue)
            {
               EasyMock.expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
               final long queueID = 1929012;
               EasyMock.expect(queue.getPersistenceID()).andReturn(queueID);
               EasyMock.expect(msg.getMessageID()).andStubReturn(i);        
               sm.storeAcknowledgeTransactional(txID, queueID, i);
            }
         }
         EasyMock.replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs / 2)
         {
            break;
         }
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.replay(refs.toArray());
      
      session.acknowledge(numRefs / 2 - 1, true);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.verify(refs.toArray());
      
      assertEquals(numRefs / 2, session.getDeliveries().size());
      
      i = numRefs / 2;
      for (Delivery del: session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }
      
      //Now ack the rest
      
      EasyMock.reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.reset(refs.toArray());
            
      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = EasyMock.createMock(ServerMessage.class);
                 
         EasyMock.expect(ref.getMessage()).andStubReturn(msg);
         EasyMock.expect(ref.getQueue()).andStubReturn(queue);
         EasyMock.expect(msg.isDurable()).andStubReturn(true);
         EasyMock.expect(queue.isDurable()).andStubReturn(true);
         EasyMock.expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         EasyMock.expect(queue.getPersistenceID()).andReturn(queueID);
         EasyMock.expect(msg.getMessageID()).andStubReturn(i);        
         sm.storeAcknowledgeTransactional(txID, queueID, i);
         EasyMock.replay(msg);
         ref.incrementDeliveryCount();
         if (i == numRefs)
         {
            break;
         }
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.replay(refs.toArray());
      
      session.acknowledge(numRefs - 1, true);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.verify(refs.toArray());
      
      assertEquals(0, session.getDeliveries().size());           
   }
   
   private void testAcknowledgeAllUpToAutoCommitAck(final boolean durableMsg, final boolean durableQueue) throws Exception
   {
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerConsumer consumer = EasyMock.createStrictMock(ServerConsumer.class);
      
      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      EasyMock.expect(sm.generateTransactionID()).andReturn(71626L);
      
      final int numRefs = 10;
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      for (int i = 0; i < numRefs; i++)
      {
         MessageReference ref = EasyMock.createStrictMock(MessageReference.class);
         
         EasyMock.expect(consumer.getClientTargetID()).andReturn(76767L);
         EasyMock.expect(ref.getMessage()).andReturn(EasyMock.createMock(ServerMessage.class));
         EasyMock.expect(ref.getDeliveryCount()).andReturn(0);
         returner.send(EasyMock.isA(ReceiveMessage.class));
         refs.add(ref);
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.replay(refs.toArray());
      
      ServerSessionImpl session = new ServerSessionImpl(conn, false, true, false,
            returner, sm, po, qs, rm, ss, pd, executor);

      for (MessageReference ref: refs)
      {
         session.handleDelivery(ref, consumer);
      }
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.verify(refs.toArray());
      
      assertEquals(numRefs, session.getDeliveries().size());
      
      int i = 0;
      for (Delivery del: session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }
      
      //Now ack half of them
      
      EasyMock.reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer);
      EasyMock.reset(refs.toArray());
      
      Queue queue = EasyMock.createMock(Queue.class); 
      for (i = 0; i < numRefs / 2; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = EasyMock.createMock(ServerMessage.class);
                 
         EasyMock.expect(ref.getMessage()).andStubReturn(msg);
         EasyMock.expect(ref.getQueue()).andStubReturn(queue);
         EasyMock.expect(msg.isDurable()).andStubReturn(durableMsg);
         EasyMock.expect(queue.isDurable()).andStubReturn(durableQueue);
         if (durableMsg && durableQueue)
         {
            EasyMock.expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
            final long queueID = 1929012;
            EasyMock.expect(queue.getPersistenceID()).andReturn(queueID);
            EasyMock.expect(msg.getMessageID()).andStubReturn(i);        
            sm.storeAcknowledge(queueID, i);
         }
         EasyMock.replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs / 2)
         {
            break;
         }
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.replay(refs.toArray());
      
      session.acknowledge(numRefs / 2 - 1, true);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.verify(refs.toArray());
      
      assertEquals(numRefs / 2, session.getDeliveries().size());
      
      i = numRefs / 2;
      for (Delivery del: session.getDeliveries())
      {
         assertEquals(i++, del.getDeliveryID());
      }
      
      //Now ack the rest
      
      EasyMock.reset(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.reset(refs.toArray());
            
      for (i = numRefs / 2; i < numRefs; i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage msg = EasyMock.createMock(ServerMessage.class);
                 
         EasyMock.expect(ref.getMessage()).andStubReturn(msg);
         EasyMock.expect(ref.getQueue()).andStubReturn(queue);
         EasyMock.expect(msg.isDurable()).andStubReturn(true);
         EasyMock.expect(queue.isDurable()).andStubReturn(true);
         EasyMock.expect(msg.decrementDurableRefCount()).andStubReturn(numRefs);
         final long queueID = 1929012;
         EasyMock.expect(queue.getPersistenceID()).andReturn(queueID);
         EasyMock.expect(msg.getMessageID()).andStubReturn(i);        
         sm.storeAcknowledge(queueID, i);
         EasyMock.replay(msg);
         queue.referenceAcknowledged(ref);
         if (i == numRefs)
         {
            break;
         }
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.replay(refs.toArray());
      
      session.acknowledge(numRefs - 1, true);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, consumer, queue);
      EasyMock.verify(refs.toArray());
      
      assertEquals(0, session.getDeliveries().size());           
   }
   
   private void testAutoCommitSend(final boolean persistent) throws Exception
   {
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference ref1 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref3 = EasyMock.createStrictMock(MessageReference.class);
      Queue queue1 = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
            
      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      EasyMock.expect(sm.generateTransactionID()).andReturn(71626L);
      
      final SimpleString dest = new SimpleString("blah");
      EasyMock.expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      
      final long messageID = 81129873;
      EasyMock.expect(sm.generateMessageID()).andReturn(messageID);
      msg.setMessageID(messageID);
      
      final long connectionID = 12734450;
      EasyMock.expect(conn.getID()).andReturn(connectionID);
      msg.setConnectionID(connectionID);
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      EasyMock.expect(po.route(msg)).andReturn(refs);
      EasyMock.expect(msg.getDurableRefCount()).andReturn(persistent ? 3 : 0);
      if (persistent)
      {
         sm.storeMessage(msg);         
      }
      EasyMock.expect(ref1.getQueue()).andReturn(queue1);
      EasyMock.expect(ref2.getQueue()).andReturn(queue2);
      EasyMock.expect(ref3.getQueue()).andReturn(queue3);
      EasyMock.expect(queue1.addLast(ref1)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue2.addLast(ref2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(queue3.addLast(ref3)).andReturn(HandleStatus.HANDLED);
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3, queue1, queue2, queue3);
                  
      ServerSessionImpl session = new ServerSessionImpl(conn, true, false, false,
                                                        returner, sm, po, qs, rm, ss, pd, executor);
      
      session.send(msg);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3, queue1, queue2, queue3);          
   }
   
   private void testNotAutoCommitSend(final boolean persistent) throws Exception
   {
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
      ServerMessage msg = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference ref1 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference ref3 = EasyMock.createStrictMock(MessageReference.class);

      final long sessionID = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      
      final long transactionID = 27334;
      EasyMock.expect(sm.generateTransactionID()).andReturn(transactionID);
      
      final SimpleString dest = new SimpleString("blah");
      EasyMock.expect(msg.getDestination()).andReturn(dest);
      ss.check(dest, CheckType.WRITE, conn);
      
      final long messageID = 81129873;
      EasyMock.expect(sm.generateMessageID()).andReturn(messageID);
      msg.setMessageID(messageID);
      
      final long connectionID = 12734450;
      EasyMock.expect(conn.getID()).andReturn(connectionID);
      msg.setConnectionID(connectionID);
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      refs.add(ref1);
      refs.add(ref2);
      refs.add(ref3);
      EasyMock.expect(po.route(msg)).andReturn(refs);
      EasyMock.expect(msg.getDurableRefCount()).andReturn(persistent ? 3 : 0);
      if (persistent)
      {
         sm.storeMessageTransactional(transactionID, msg);         
      }

      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3);
                  
      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, false,
                                                        returner, sm, po, qs, rm, ss, pd, executor);
      
      session.send(msg);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor, msg, ref1, ref2, ref3);            
   }
   
   
   private void testConstructor(final boolean xa) throws Exception
   {     
      ServerConnection conn = EasyMock.createStrictMock(ServerConnection.class);      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      Executor executor = EasyMock.createStrictMock(Executor.class);
            
      final long id = 102981029;
      
      EasyMock.expect(pd.generateID()).andReturn(id);
      
      if (!xa)
      {        
         EasyMock.expect(sm.generateTransactionID()).andReturn(71626L);
      }
      
      EasyMock.replay(conn, returner, sm, po, qs, rm, ss, pd, executor);
      
      ServerSessionImpl session = new ServerSessionImpl(conn, false, false, xa,
                                                        returner, sm, po, qs, rm, ss, pd, executor);
      
      EasyMock.verify(conn, returner, sm, po, qs, rm, ss, pd, executor);
      
      assertEquals(id, session.getID());
   }
}
