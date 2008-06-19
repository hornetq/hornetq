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

import org.easymock.EasyMock;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerConnectionImpl;
import org.jboss.messaging.core.server.impl.ServerSessionPacketHandler;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.SimpleString;

import java.util.concurrent.Executor;

/**
 * 
 * A ServerConnectionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerConnectionImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ServerConnectionImplTest.class);
   
   public void testConstructor() throws Exception
   {
      createConnection(91821982, "oaksaoks", "asokdasod", 9120912);                  
   }
         
   public void testAddRemoveTemporaryDestinations() throws Exception
   {
      ServerConnectionImpl conn = createConnection(8172817, "okooko", "oaksaoks", 812981);   
      
      final SimpleString address1 = new SimpleString("ashaijsaisj");
      final SimpleString address2 = new SimpleString("iuhasiasa");
      final SimpleString address3 = new SimpleString("owqdqwoijd");
      
      assertEquals(0, conn.getTemporaryDestinations().size());
      conn.addTemporaryDestination(address1);
      assertEquals(1, conn.getTemporaryDestinations().size());
      assertTrue(conn.getTemporaryDestinations().contains(address1));
      conn.addTemporaryDestination(address2);
      assertEquals(2, conn.getTemporaryDestinations().size());
      assertTrue(conn.getTemporaryDestinations().contains(address1));
      assertTrue(conn.getTemporaryDestinations().contains(address2));
      conn.addTemporaryDestination(address3);
      assertEquals(3, conn.getTemporaryDestinations().size());
      assertTrue(conn.getTemporaryDestinations().contains(address1));
      assertTrue(conn.getTemporaryDestinations().contains(address2));
      assertTrue(conn.getTemporaryDestinations().contains(address3));
      try
      {
         conn.addTemporaryDestination(address3);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      assertEquals(3, conn.getTemporaryDestinations().size());
      
      conn.removeTemporaryDestination(address3);
      assertEquals(2, conn.getTemporaryDestinations().size());
      assertTrue(conn.getTemporaryDestinations().contains(address1));
      assertTrue(conn.getTemporaryDestinations().contains(address2));
      conn.removeTemporaryDestination(address2);
      assertEquals(1, conn.getTemporaryDestinations().size());
      assertTrue(conn.getTemporaryDestinations().contains(address1));
      conn.removeTemporaryDestination(address1);
      assertEquals(0, conn.getTemporaryDestinations().size());
      
      try
      {
         conn.removeTemporaryDestination(address1);
         fail("Should throw exeception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         conn.removeTemporaryDestination(new SimpleString("iwjwiojjoiqwdjqw"));
         fail("Should throw exeception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }        
   }
   
   public void testAddRemoveTemporaryQueues() throws Exception
   {
      ServerConnectionImpl conn = createConnection(8172178, "ijijji", "aoksoaks", 99182);   
      
      final Queue queue1 = EasyMock.createStrictMock(Queue.class);
      final Queue queue2 = EasyMock.createStrictMock(Queue.class);
      final Queue queue3 = EasyMock.createStrictMock(Queue.class);
      
      assertEquals(0, conn.getTemporaryQueues().size());
      conn.addTemporaryQueue(queue1);
      assertEquals(1, conn.getTemporaryQueues().size());
      assertTrue(conn.getTemporaryQueues().contains(queue1));
      conn.addTemporaryQueue(queue2);
      assertEquals(2, conn.getTemporaryQueues().size());
      assertTrue(conn.getTemporaryQueues().contains(queue1));
      assertTrue(conn.getTemporaryQueues().contains(queue2));
      conn.addTemporaryQueue(queue3);
      assertEquals(3, conn.getTemporaryQueues().size());
      assertTrue(conn.getTemporaryQueues().contains(queue1));
      assertTrue(conn.getTemporaryQueues().contains(queue2));
      assertTrue(conn.getTemporaryQueues().contains(queue3));
      try
      {
         conn.addTemporaryQueue(queue3);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      assertEquals(3, conn.getTemporaryQueues().size());
      
      conn.removeTemporaryQueue(queue3);
      assertEquals(2, conn.getTemporaryQueues().size());
      assertTrue(conn.getTemporaryQueues().contains(queue1));
      assertTrue(conn.getTemporaryQueues().contains(queue2));
      conn.removeTemporaryQueue(queue2);
      assertEquals(1, conn.getTemporaryQueues().size());
      assertTrue(conn.getTemporaryQueues().contains(queue1));
      conn.removeTemporaryQueue(queue1);
      assertEquals(0, conn.getTemporaryQueues().size());
      
      try
      {
         conn.removeTemporaryQueue(queue1);
         fail("Should throw exeception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         conn.removeTemporaryQueue(EasyMock.createStrictMock(Queue.class));
         fail("Should throw exeception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
        
   }
   
   public void testCreateSession() throws Exception
   {
      testCreateSession(false, false, false);
      testCreateSession(true, true, true);
   }
   
   public void testStartStop() throws Exception
   {      
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      ConnectionManager cm = EasyMock.createStrictMock(ConnectionManager.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      ExecutorFactory ef = EasyMock.createStrictMock(ExecutorFactory.class);
      
      final long id = 120912;
      
      EasyMock.expect(pd.generateID()).andReturn(id);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef);
       
      ServerConnectionImpl conn = new ServerConnectionImpl("huihuh", "ookkok",
                     192818, po, cm, pd, sm, qs, rm, ss, ef);
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef);
      
      ServerSession session1 = EasyMock.createStrictMock(ServerSession.class);
      ServerSession session2 = EasyMock.createStrictMock(ServerSession.class);
      ServerSession session3 = EasyMock.createStrictMock(ServerSession.class);
      
      conn.addSession(session1);
      conn.addSession(session2);
      conn.addSession(session3);
      
      assertEquals(3, conn.getSessions().size());
      
      assertFalse(conn.isStarted());
      
      EasyMock.reset(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      session1.setStarted(true);
      session2.setStarted(true);
      session3.setStarted(true);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      conn.start();
      
      assertTrue(conn.isStarted());
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      EasyMock.reset(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      session1.setStarted(false);
      session2.setStarted(false);
      session3.setStarted(false);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
            
      conn.stop();
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      assertFalse(conn.isStarted());      
   }
   
   public void testClose() throws Exception
   {
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      ConnectionManager cm = EasyMock.createStrictMock(ConnectionManager.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      ExecutorFactory ef = EasyMock.createStrictMock(ExecutorFactory.class);
      
      ServerSession session1 = EasyMock.createStrictMock(ServerSession.class);
      ServerSession session2 = EasyMock.createStrictMock(ServerSession.class);
      ServerSession session3 = EasyMock.createStrictMock(ServerSession.class);
      
      final long id = 91829182;
      EasyMock.expect(pd.generateID()).andReturn(id);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      final long remotingClientSessionID = 81728172;
      
      ServerConnectionImpl conn = new ServerConnectionImpl("huihuh", "ookkok",
            remotingClientSessionID, po, cm, pd, sm, qs, rm, ss, ef);
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      conn.addSession(session1);
      conn.addSession(session2);
      conn.addSession(session3);
      
      SimpleString address1 = new SimpleString("uyuyyu");
      SimpleString address2 = new SimpleString("ioajiojad");
      SimpleString address3 = new SimpleString("isjqijs");
      conn.addTemporaryDestination(address1);
      conn.addTemporaryDestination(address2);
      conn.addTemporaryDestination(address3);
      
      Queue queue1 = EasyMock.createMock(Queue.class);
      Queue queue2 = EasyMock.createMock(Queue.class);
      Queue queue3 = EasyMock.createMock(Queue.class);
      conn.addTemporaryQueue(queue1);
      conn.addTemporaryQueue(queue2);
      conn.addTemporaryQueue(queue3);
      
      assertEquals(3, conn.getSessions().size());
      assertEquals(3, conn.getTemporaryDestinations().size());
      assertEquals(3, conn.getTemporaryQueues().size());
      
      assertFalse(conn.isClosed());
      
      EasyMock.reset(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3);
      
      session1.close();
      session2.close();
      session3.close();
      
      EasyMock.expect(queue1.getName()).andReturn(new SimpleString("uyuyh1"));
      EasyMock.expect(queue2.getName()).andReturn(new SimpleString("uyuyh2"));
      EasyMock.expect(queue3.getName()).andReturn(new SimpleString("uyuyh3"));
      
      EasyMock.checkOrder(po, false);
      
      Binding binding1 = EasyMock.createStrictMock(Binding.class);
      Binding binding2 = EasyMock.createStrictMock(Binding.class);
      Binding binding3 = EasyMock.createStrictMock(Binding.class);
      
      EasyMock.expect(po.getBinding(new SimpleString("uyuyh1"))).andReturn(binding1);
      EasyMock.expect(po.getBinding(new SimpleString("uyuyh2"))).andReturn(binding2);
      EasyMock.expect(po.getBinding(new SimpleString("uyuyh3"))).andReturn(binding3);
      
      EasyMock.expect(binding1.getAddress()).andReturn(new SimpleString("ahshs1"));
      EasyMock.expect(binding2.getAddress()).andReturn(new SimpleString("ahshs2"));
      EasyMock.expect(binding3.getAddress()).andReturn(new SimpleString("ahshs3"));
            
      EasyMock.expect(po.removeBinding(new SimpleString("uyuyh1"))).andReturn(null);
      EasyMock.expect(po.removeBinding(new SimpleString("uyuyh2"))).andReturn(null);
      EasyMock.expect(po.removeBinding(new SimpleString("uyuyh3"))).andReturn(null);
      
      EasyMock.expect(po.removeDestination(new SimpleString("ahshs1"), true)).andReturn(true);
      EasyMock.expect(po.removeDestination(new SimpleString("ahshs2"), true)).andReturn(true);
      EasyMock.expect(po.removeDestination(new SimpleString("ahshs3"), true)).andReturn(true);
      
      EasyMock.expect(po.removeDestination(address1, true)).andReturn(true);
      EasyMock.expect(po.removeDestination(address2, true)).andReturn(true);
      EasyMock.expect(po.removeDestination(address3, true)).andReturn(true);
      
      EasyMock.expect(cm.unregisterConnection(remotingClientSessionID, conn)).andReturn(null);
      
      pd.unregister(id);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3, queue1, queue2, queue3,
            binding1, binding2, binding3);
      
      conn.close();
      
      assertTrue(conn.isClosed());
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3, queue1, queue2, queue3,
            binding1, binding2, binding3);
      
      EasyMock.reset(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3, queue1, queue2, queue3,
            binding1, binding2, binding3);  
      
      //Closing again should do nothing
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3, queue1, queue2, queue3,
            binding1, binding2, binding3);
      
      conn.close();
      
      assertTrue(conn.isClosed());
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef, session1, session2, session3, queue1, queue2, queue3,
            binding1, binding2, binding3);
   }
   
   // Private ----------------------------------------------------------------------------------------
   
   private void testCreateSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks) throws Exception
   {
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      ConnectionManager cm = EasyMock.createStrictMock(ConnectionManager.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      ExecutorFactory ef = EasyMock.createStrictMock(ExecutorFactory.class);
      
      final long id = 91829182;
      
      EasyMock.expect(pd.generateID()).andReturn(id);
                 
      EasyMock.expect(ef.getExecutor()).andReturn(EasyMock.createMock(Executor.class));
      
      final long sessionID = 18927;
      
      EasyMock.expect(pd.generateID()).andReturn(sessionID);
      
      if (!xa)
      {
         EasyMock.expect(sm.generateTransactionID()).andReturn(8172L);
      }
      
      pd.register(EasyMock.isA(ServerSessionPacketHandler.class));
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef);
    
      ServerConnectionImpl conn = new ServerConnectionImpl("huihuh", "ookkok",
            8172718, po, cm, pd, sm, qs, rm, ss, ef);
      
      PacketReturner returner = EasyMock.createStrictMock(PacketReturner.class);
    
      ConnectionCreateSessionResponseMessage resp = conn.createSession(xa, autoCommitSends, autoCommitAcks, returner);
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef);      
      
      assertEquals(sessionID, resp.getSessionID());
      
      assertEquals(1, conn.getSessions().size());     
   }
   
   private ServerConnectionImpl createConnection(final long id, final String username, final String password,
         final long clientSessionID)
   {
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      ConnectionManager cm = EasyMock.createStrictMock(ConnectionManager.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> qs = EasyMock.createStrictMock(HierarchicalRepository.class);
      ResourceManager rm = EasyMock.createStrictMock(ResourceManager.class);
      SecurityStore ss = EasyMock.createStrictMock(SecurityStore.class);
      ExecutorFactory ef = EasyMock.createStrictMock(ExecutorFactory.class);
      
      EasyMock.expect(pd.generateID()).andReturn(id);
      
      EasyMock.replay(po, cm, pd, sm, qs, rm, ss, ef);
       
      ServerConnectionImpl conn = new ServerConnectionImpl(username, password,
                     clientSessionID, po, cm, pd, sm, qs, rm, ss, ef);
      
      EasyMock.verify(po, cm, pd, sm, qs, rm, ss, ef);
      
      assertEquals(username, conn.getUsername());
      assertEquals(password, conn.getPassword());
      assertEquals(clientSessionID, conn.getClientSessionID());
      assertEquals(id, conn.getID());
      
      assertTrue(conn.getSessions().isEmpty());
      assertTrue(conn.getTemporaryDestinations().isEmpty());
      assertTrue(conn.getTemporaryQueues().isEmpty());
      
      return conn;
   }
}
