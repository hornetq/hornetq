/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Set;

import javax.management.NotificationBroadcasterSupport;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.impl.MessagingServerControl;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * t
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessagingServerControlTest extends UnitTestCase
{
   private PostOffice postOffice;

   private StorageManager storageManager;

   private Configuration configuration;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private ResourceManager resourceManager;

   private MessagingServer server;

   private MessageCounterManager messageCounterManager;

   private RemotingService remotingService;
   
   private QueueFactory queueFactory;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFoo()
   {      
   }
   
//   public void testIsStarted() throws Exception
//   {
//      boolean started = randomBoolean();
//
//      expect(server.isStarted()).andStubReturn(started);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(started, control.isStarted());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetVersion() throws Exception
//   {
//
//      String fullVersion = randomString();
//      Version version = createMock(Version.class);
//      expect(version.getFullVersion()).andReturn(fullVersion);
//      expect(server.getVersion()).andStubReturn(version);
//      replayMockedAttributes();
//      replay(version);
//
//      MessagingServerControl control = createControl();
//      assertEquals(fullVersion, control.getVersion());
//
//      verify(version);
//      verifyMockedAttributes();
//   }
//
//   public void testGetBindingsDirectory() throws Exception
//   {
//      String dir = randomString();
//
//      expect(configuration.getBindingsDirectory()).andReturn(dir);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(dir, control.getBindingsDirectory());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetInterceptorClassNames() throws Exception
//   {
//      List<String> list = new ArrayList<String>();
//      list.add(randomString());
//
//      expect(configuration.getInterceptorClassNames()).andReturn(list);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(list, control.getInterceptorClassNames());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetJournalDirectory() throws Exception
//   {
//      String dir = randomString();
//
//      expect(configuration.getJournalDirectory()).andReturn(dir);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(dir, control.getJournalDirectory());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetJournalFileSize() throws Exception
//   {
//      int size = randomInt();
//
//      expect(configuration.getJournalFileSize()).andReturn(size);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(size, control.getJournalFileSize());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetJournalMaxAIO() throws Exception
//   {
//      int max = randomInt();
//
//      expect(configuration.getJournalMaxAIO()).andReturn(max);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(max, control.getJournalMaxAIO());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetJournalMinFiles() throws Exception
//   {
//      int minFiles = randomInt();
//
//      expect(configuration.getJournalMinFiles()).andReturn(minFiles);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(minFiles, control.getJournalMinFiles());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetJournalType() throws Exception
//   {
//      expect(configuration.getJournalType()).andReturn(JournalType.ASYNCIO);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(JournalType.ASYNCIO.toString(), control.getJournalType());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetScheduledThreadPoolMaxSize() throws Exception
//   {
//      int size = randomInt();
//
//      expect(configuration.getScheduledThreadPoolMaxSize()).andReturn(size);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(size, control.getScheduledThreadPoolMaxSize());
//
//      verifyMockedAttributes();
//   }
//
//   public void testGetSecurityInvalidationInterval() throws Exception
//   {
//      long interval = randomLong();
//
//      expect(configuration.getSecurityInvalidationInterval()).andReturn(interval);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(interval, control.getSecurityInvalidationInterval());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsClustered() throws Exception
//   {
//      boolean clustered = randomBoolean();
//
//      expect(configuration.isClustered()).andReturn(clustered);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(clustered, control.isClustered());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsCreateBindingsDir() throws Exception
//   {
//      boolean createBindingsDir = randomBoolean();
//
//      expect(configuration.isCreateBindingsDir()).andReturn(createBindingsDir);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(createBindingsDir, control.isCreateBindingsDir());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsCreateJournalDir() throws Exception
//   {
//      boolean createJournalDir = randomBoolean();
//
//      expect(configuration.isCreateJournalDir()).andReturn(createJournalDir);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(createJournalDir, control.isCreateJournalDir());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsJournalSyncNonTransactional() throws Exception
//   {
//      boolean journalSyncNonTransactional = randomBoolean();
//
//      expect(configuration.isJournalSyncNonTransactional()).andReturn(journalSyncNonTransactional);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(journalSyncNonTransactional, control.isJournalSyncNonTransactional());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsJournalSyncTransactional() throws Exception
//   {
//      boolean journalSyncTransactional = randomBoolean();
//
//      expect(configuration.isJournalSyncTransactional()).andReturn(journalSyncTransactional);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(journalSyncTransactional, control.isJournalSyncTransactional());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsRequireDestinations() throws Exception
//   {
//      boolean requireDestinations = randomBoolean();
//
//      expect(configuration.isRequireDestinations()).andReturn(requireDestinations);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(requireDestinations, control.isRequireDestinations());
//
//      verifyMockedAttributes();
//   }
//
//   public void testIsSecurityEnabled() throws Exception
//   {
//      boolean securityEnabled = randomBoolean();
//
//      expect(configuration.isSecurityEnabled()).andReturn(securityEnabled);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(securityEnabled, control.isSecurityEnabled());
//
//      verifyMockedAttributes();
//   }
//
//   public void testAddDestination() throws Exception
//   {
//      String address = randomString();
//      boolean added = randomBoolean();
//
//      expect(postOffice.addDestination(new SimpleString(address), false)).andReturn(added);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//
//      assertEquals(added, control.addAddress(address));
//
//      verifyMockedAttributes();
//   }
//
//   public void testRemoveAddress() throws Exception
//   {
//      String address = randomString();
//      boolean removed = randomBoolean();
//
//      expect(postOffice.removeDestination(new SimpleString(address), false)).andReturn(removed);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(removed, control.removeAddress(address));
//
//      verifyMockedAttributes();
//   }

//   public void testCreateQueue() throws Exception
//   {
//      String address = randomString();
//      String name = randomString();
//
//      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
//      Binding newBinding = createMock(Binding.class);
//      expect(postOffice.addQueueBinding(new SimpleString(address), new SimpleString(name), null, true, false, false)).andReturn(newBinding);
//      replayMockedAttributes();
//      replay(newBinding);
//
//      MessagingServerControl control = createControl();
//      control.createQueue(address, name);
//
//      verifyMockedAttributes();
//      verify(newBinding);
//   }
//
//   public void testCreateQueueWithAllParameters() throws Exception
//   {
//      String address = randomString();
//      String name = randomString();
//      String filter = "color = 'green'";
//      boolean durable = true;
//      boolean temporary = false;
//      boolean exclusive = false;
//
//      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
//      Binding newBinding = createMock(Binding.class);
//      expect(postOffice.addQueueBinding(eq(new SimpleString(address)),
//                                        eq(new SimpleString(name)),
//                                        isA(Filter.class),
//                                        eq(durable),
//                                        eq(temporary),
//                                        eq(exclusive))).andReturn(newBinding);
//      replayMockedAttributes();
//      replay(newBinding);
//
//      MessagingServerControl control = createControl();
//      control.createQueue(address, name, filter, durable);
//
//      verify(newBinding);
//      verifyMockedAttributes();
//   }
//
//   public void testCreateQueueWithEmptyFilter() throws Exception
//   {
//      String address = randomString();
//      String name = randomString();
//      String filter = "";
//      boolean durable = true;
//      boolean temporary = false;
//
//      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
//      Binding newBinding = createMock(Binding.class);
//      expect(postOffice.addQueueBinding(new SimpleString(address),
//                                        new SimpleString(name),
//                                        null,
//                                        durable,
//                                        temporary,
//                                        false)).andReturn(newBinding);
//      replay(newBinding);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      control.createQueue(address, name, filter, durable);
//
//      verify(newBinding);
//      verifyMockedAttributes();
//   }
//
//   public void testCreateQueueWithNullFilter() throws Exception
//   {
//      String address = randomString();
//      String name = randomString();
//      String filter = null;
//      boolean durable = true;
//      boolean temporary = false;
//
//      expect(postOffice.getBinding(new SimpleString(address))).andReturn(null);
//      Binding newBinding = createMock(Binding.class);
//      expect(postOffice.addQueueBinding(new SimpleString(address),
//                                        new SimpleString(name),
//                                        null,
//                                        durable,
//                                        temporary,
//                                        false)).andReturn(newBinding);
//      replay(newBinding);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      control.createQueue(address, name, filter, durable);
//
//      verify(newBinding);
//      verifyMockedAttributes();
//   }
//
//   public void testDestroyQueueAndReceiveNotification() throws Exception
//   {
//      String name = randomString();
//
//      Binding binding = createMock(Binding.class);
//      Queue queue = createMock(Queue.class);
//      expect(queue.getName()).andReturn(new SimpleString(name));
//      expect(binding.getBindable()).andReturn(queue);
//      expect(postOffice.getBinding(new SimpleString(name))).andReturn(binding);
//      expect(queue.deleteAllReferences(storageManager)).andReturn(randomPositiveInt());
//      expect(postOffice.removeBinding(new SimpleString(name))).andReturn(binding);
//      replayMockedAttributes();
//      replay(binding, queue);
//
//      MessagingServerControl control = createControl();
//      control.destroyQueue(name);
//
//      verify(binding, queue);
//      verifyMockedAttributes();
//   }

//   public void testGetConnectionCount() throws Exception
//   {
//      int count = randomInt();
//
//      expect(server.getConnectionCount()).andReturn(count);
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//      assertEquals(count, control.getConnectionCount());
//
//      verifyMockedAttributes();
//   }
//
//   public void testListPreparedTransactions() throws Exception
//   {
//      Xid xid1 = randomXid();
//      Xid xid2 = randomXid();
//      Xid xid3 = randomXid();
//      long oldestCreationTime = System.currentTimeMillis();
//      long midCreationTime = oldestCreationTime + 3600;
//      long newestCreationTime = midCreationTime + 3600;
//
//      Map<Xid, Long> xids = new HashMap<Xid, Long>();
//      xids.put(xid3, newestCreationTime);
//      xids.put(xid1, oldestCreationTime);
//      xids.put(xid2, midCreationTime);
//
//      expect(resourceManager.getPreparedTransactionsWithCreationTime()).andStubReturn(xids);
//
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//
//      String[] preparedTransactions = control.listPreparedTransactions();
//
//      assertEquals(3, preparedTransactions.length);
//      // sorted by date, oldest first
//      System.out.println(preparedTransactions[0]);
//      System.out.println(preparedTransactions[1]);
//      System.out.println(preparedTransactions[2]);
//      assertTrue(preparedTransactions[0].contains(xid1.toString()));
//      assertTrue(preparedTransactions[1].contains(xid2.toString()));
//      assertTrue(preparedTransactions[2].contains(xid3.toString()));
//
//      verifyMockedAttributes();
//   }
//
//   public void testCommitPreparedTransactionWithKnownPreparedTransaction() throws Exception
//   {
//      Xid xid = randomXid();
//      String transactionAsBase64 = XidImpl.toBase64String(xid);
//      Transaction tx = createMock(Transaction.class);
//
//      expect(resourceManager.getPreparedTransactions()).andReturn(Arrays.asList(xid));
//      expect(resourceManager.removeTransaction(xid)).andReturn(tx);
//      tx.commit();
//
//      replayMockedAttributes();
//      replay(tx);
//
//      MessagingServerControl control = createControl();
//
//      assertTrue(control.commitPreparedTransaction(transactionAsBase64));
//
//      verifyMockedAttributes();
//      verify(tx);
//   }
//
//   public void testCommitPreparedTransactionWithUnknownPreparedTransaction() throws Exception
//   {
//      Xid xid = randomXid();
//      String transactionAsBase64 = XidImpl.toBase64String(xid);
//
//      expect(resourceManager.getPreparedTransactions()).andStubReturn(Collections.emptyList());
//
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//
//      assertFalse(control.commitPreparedTransaction(transactionAsBase64));
//
//      verifyMockedAttributes();
//   }
//
//   public void testRollbackPreparedTransactionWithKnownPreparedTransaction() throws Exception
//   {
//      Xid xid = randomXid();
//      String transactionAsBase64 = XidImpl.toBase64String(xid);
//      Transaction tx = createMock(Transaction.class);
//
//      expect(resourceManager.getPreparedTransactions()).andReturn(Arrays.asList(xid));
//      expect(resourceManager.removeTransaction(xid)).andReturn(tx);
//      tx.rollback();
//
//      replayMockedAttributes();
//      replay(tx);
//
//      MessagingServerControl control = createControl();
//
//      assertTrue(control.rollbackPreparedTransaction(transactionAsBase64));
//
//      verifyMockedAttributes();
//      verify(tx);
//   }
//
//   public void testRollbackPreparedTransactionWithUnknownPreparedTransaction() throws Exception
//   {
//      Xid xid = randomXid();
//      String transactionAsBase64 = XidImpl.toBase64String(xid);
//
//      expect(resourceManager.getPreparedTransactions()).andStubReturn(Collections.emptyList());
//
//      replayMockedAttributes();
//
//      MessagingServerControl control = createControl();
//
//      assertFalse(control.rollbackPreparedTransaction(transactionAsBase64));
//
//      verifyMockedAttributes();
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      postOffice = createMock(PostOffice.class);
      storageManager = createMock(StorageManager.class);
      configuration = createMock(Configuration.class);
      expect(configuration.isMessageCounterEnabled()).andReturn(false);
      securityRepository = createMock(HierarchicalRepository.class);
      addressSettingsRepository = createMock(HierarchicalRepository.class);
      queueFactory = createMock(QueueFactory.class);
      remotingService = createMock(RemotingService.class);
      resourceManager = createMock(ResourceManager.class);
      server = createMock(MessagingServer.class);
      messageCounterManager = createMock(MessageCounterManager.class);
   }

   @Override
   protected void tearDown() throws Exception
   {
      postOffice = null;
      storageManager = null;
      configuration = null;
      securityRepository = null;
      addressSettingsRepository = null;
      resourceManager = null;
      remotingService = null;
      server = null;
      messageCounterManager = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   private MessagingServerControl createControl() throws Exception
   {
      MessagingServerControl control = new MessagingServerControl(postOffice,
                                                                  storageManager,
                                                                  configuration,                                                            
                                                                  resourceManager,
                                                                  remotingService,
                                                                  server,
                                                                  messageCounterManager,
                                                                  new NotificationBroadcasterSupport(),
                                                                  queueFactory);
      return control;
   }

   private void replayMockedAttributes()
   {
      replay(postOffice,
             storageManager,
             configuration,
             securityRepository,
             addressSettingsRepository,
             resourceManager,
             remotingService,
             server,
             messageCounterManager);
   }

   private void verifyMockedAttributes()
   {
      verify(postOffice,
             storageManager,
             configuration,
             securityRepository,
             addressSettingsRepository,
             resourceManager,
             remotingService,
             server,
             messageCounterManager);
   }

   // Inner classes -------------------------------------------------
}
