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

package org.jboss.messaging.tests.unit.core.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 */
public class MessagingServerManagementImplTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private PostOffice mockPostOffice;
   private StorageManager mockStorageManager;
   private Configuration mockConfiguration;
   private HierarchicalRepository<Set<Role>> mockSecurityRepository;
   private HierarchicalRepository<QueueSettings> mockQueueSettingsRepository;
   private MessagingServer mockServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      mockPostOffice = EasyMock.createNiceMock(PostOffice.class);
      mockStorageManager = EasyMock.createNiceMock(StorageManager.class);
      mockConfiguration = EasyMock.createNiceMock(Configuration.class);
      mockSecurityRepository = EasyMock
            .createNiceMock(HierarchicalRepository.class);
      mockQueueSettingsRepository = EasyMock
            .createNiceMock(HierarchicalRepository.class);
      mockServer = EasyMock.createNiceMock(MessagingServer.class);

   }

   public void testcreateQueue() throws Exception
   {
      SimpleString destination = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(null);

      EasyMock.expect(
            mockPostOffice.addBinding(destination, name, null, true, false))
            .andReturn(null);

      EasyMock.replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();

      impl.createQueue(destination, name);

      EasyMock.verify(mockPostOffice);

      EasyMock.reset(mockPostOffice);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(
            EasyMock.createNiceMock(Binding.class));

      EasyMock.replay(mockPostOffice);

      impl.createQueue(destination, name);

      EasyMock.verify(mockPostOffice);

   }

   public void testCreateQueueWithFullParameters() throws Exception
   {
      SimpleString destination = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      SimpleString filterStr = new SimpleString("color = 'green'");
      boolean durable = false;
      boolean temporary = true;

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(null);

      EasyMock.expect(
            mockPostOffice.addBinding(eq(destination), eq(name),
                  isA(Filter.class), eq(durable), eq(temporary))).andReturn(
            null);

      EasyMock.replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();

      impl.createQueue(destination, name, filterStr, durable, temporary);

      EasyMock.verify(mockPostOffice);

      EasyMock.reset(mockPostOffice);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(
            EasyMock.createNiceMock(Binding.class));

      EasyMock.replay(mockPostOffice);

      impl.createQueue(destination, name, filterStr, durable, temporary);

      EasyMock.verify(mockPostOffice);

   }

   public void testConnectionCount() throws Exception
   {      
      MessagingServerManagementImpl impl = createImpl();

      assertEquals(impl.getConnectionCount(), 0);      
   }

   public void testDestroyQueue() throws Exception
   {
      SimpleString name = RandomUtil.randomSimpleString();

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      Binding binding = EasyMock.createMock(Binding.class);

      Queue queue = EasyMock.createMock(Queue.class);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(binding);
      EasyMock.expect(binding.getQueue()).andReturn(queue);

      queue.deleteAllReferences(mockStorageManager);

      EasyMock.expect(queue.getName()).andReturn(name).atLeastOnce();

      EasyMock.expect(mockPostOffice.removeBinding(name)).andReturn(binding);

      EasyMock.replay(mockPostOffice, binding, queue);

      MessagingServerManagementImpl impl = createImpl();

      impl.destroyQueue(name);

      EasyMock.verify(mockPostOffice, binding, queue);

      EasyMock.reset(mockPostOffice, binding, queue);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(null);

      EasyMock.replay(mockPostOffice, binding, queue);

      impl.destroyQueue(name);

      EasyMock.verify(mockPostOffice, binding, queue);

   }

   public void testAddDestination() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.addDestination(address, false)).andReturn(
            false);

      EasyMock.replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.addDestination(address));

      EasyMock.reset(mockPostOffice);

      EasyMock.expect(mockPostOffice.addDestination(address, false)).andReturn(
            true);

      EasyMock.replay(mockPostOffice);

      assertTrue(impl.addDestination(address));

      EasyMock.verify(mockPostOffice);

   }

   public void testRemoveDestination() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.removeDestination(address, false))
            .andReturn(false);

      EasyMock.replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.removeDestination(address));

      EasyMock.reset(mockPostOffice);

      EasyMock.expect(mockPostOffice.removeDestination(address, false))
            .andReturn(true);

      EasyMock.replay(mockPostOffice);

      assertTrue(impl.removeDestination(address));

      EasyMock.verify(mockPostOffice);

   }

   public void testRemoveAllMessagesForAddress() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();

      int numberOfQueues = 10;

      ArrayList<Queue> queues = new ArrayList<Queue>();
      ArrayList<Binding> bindings = new ArrayList<Binding>();

      for (int i = 0; i < numberOfQueues; i++)
      {
         Queue queue = EasyMock.createMock(Queue.class);
         queues.add(queue);

         Binding binding = EasyMock.createMock(Binding.class);
         bindings.add(binding);

         EasyMock.expect(binding.getQueue()).andReturn(queue);

         queue.deleteAllReferences(mockStorageManager);
      }

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.getBindingsForAddress(address)).andReturn(
            bindings);

      EasyMock.replay(mockPostOffice);

      EasyMock.replay(queues.toArray());

      EasyMock.replay(bindings.toArray());

      MessagingServerManagementImpl impl = createImpl();

      impl.removeAllMessagesForAddress(address);

      EasyMock.verify(mockPostOffice);

      EasyMock.verify(queues.toArray());

      EasyMock.verify(bindings.toArray());

   }

   public void testRemoveMessageFromQueue() throws Exception
   {
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      expect(queue.deleteReference(messageID, mockStorageManager)).andReturn(
            true);

      replay(mockPostOffice, binding, queue, mockStorageManager);

      MessagingServerManagementImpl impl = createImpl();
      assertTrue(impl.removeMessageFromQueue(messageID, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager);
   }

   public void testRemoveMessageForUnkownAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      expect(mockPostOffice.getBinding(address)).andReturn(null);

      replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.removeMessageFromQueue(messageID, address));

      verify(mockPostOffice);
   }

   public void testExpireMessage() throws Exception
   {
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      expect(
            queue.expireMessage(messageID, mockStorageManager, mockPostOffice,
                  mockQueueSettingsRepository)).andReturn(true);

      replay(mockPostOffice, binding, queue, mockStorageManager);

      MessagingServerManagementImpl impl = createImpl();
      assertTrue(impl.expireMessage(messageID, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager);
   }

   public void testExpireMessageForUnkownQueueName() throws Exception
   {
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(null);

      replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.expireMessage(messageID, queueName));

      verify(mockPostOffice);
   }

   public void testExpireMessages() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      expect(queue.list(filter)).andReturn(refs);
      expect(
            queue.expireMessage(messageID, mockStorageManager, mockPostOffice,
                  mockQueueSettingsRepository)).andReturn(true);

      replay(mockPostOffice, binding, queue, mockStorageManager, ref, message);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(1, impl.expireMessages(filter, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager, ref, message);
   }

   public void testExpireMessagesForUnknownQueueName() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(null);

      replay(mockPostOffice, binding, queue, mockStorageManager);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(0, impl.expireMessages(filter, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager);
   }

   public void testSendMessagesToDLQ() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      expect(queue.list(filter)).andReturn(refs);
      expect(
            queue.sendMessageToDLQ(messageID, mockStorageManager,
                  mockPostOffice, mockQueueSettingsRepository)).andReturn(true);

      replay(mockPostOffice, binding, queue, mockStorageManager, ref, message);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(1, impl.sendMessagesToDLQ(filter, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager, ref, message);
   }

   public void testSendMessagesToDLQForUnknownQueueName() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();

      mockPostOffice = createMock(PostOffice.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(null);

      replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(0, impl.sendMessagesToDLQ(filter, queueName));

      verify(mockPostOffice);
   }

   public void testChangeMessagesPriority() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();
      long messageID = randomLong();
      byte newPriority = (byte) 9;

      mockPostOffice = createMock(PostOffice.class);
      Binding binding = createMock(Binding.class);
      Queue queue = createMock(Queue.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(binding);
      expect(binding.getQueue()).andReturn(queue);
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = createMock(MessageReference.class);
      refs.add(ref);
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getMessageID()).andReturn(messageID);
      expect(ref.getMessage()).andReturn(message);
      expect(queue.list(filter)).andReturn(refs);
      expect(
            queue.changeMessagePriority(messageID, newPriority,
                  mockStorageManager, mockPostOffice,
                  mockQueueSettingsRepository)).andReturn(true);

      replay(mockPostOffice, binding, queue, mockStorageManager, ref, message);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(1, impl.changeMessagesPriority(filter, newPriority, queueName));

      verify(mockPostOffice, binding, queue, mockStorageManager, ref, message);
   }

   public void testChangeMessagesPriorityForUnknownQueueName() throws Exception
   {
      Filter filter = new FilterImpl(new SimpleString("color = 'green'"));
      SimpleString queueName = randomSimpleString();
      byte newPriority = (byte) 9;

      mockPostOffice = createMock(PostOffice.class);
      expect(mockPostOffice.getBinding(queueName)).andReturn(null);

      replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(0, impl.changeMessagesPriority(filter, newPriority, queueName));

      verify(mockPostOffice);
   }

   public void testGetQueuesForAddress() throws Exception
   {

      int numberOfQueues = 10;

      ArrayList<Queue> queues = new ArrayList<Queue>();
      ArrayList<Binding> bindings = new ArrayList<Binding>();

      for (int i = 0; i < numberOfQueues; i++)
      {
         Queue queue = EasyMock.createMock(Queue.class);

         queues.add(queue);

         Binding binding = EasyMock.createMock(Binding.class);

         bindings.add(binding);

         EasyMock.expect(binding.getQueue()).andReturn(queue);
      }

      SimpleString address = RandomUtil.randomSimpleString();

      EasyMock.expect(mockPostOffice.getBindingsForAddress(address)).andReturn(
            bindings);

      EasyMock.replay(mockPostOffice);
      EasyMock.replay(queues.toArray());
      EasyMock.replay(bindings.toArray());

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(numberOfQueues, impl.getQueuesForAddress(address).size());

      EasyMock.verify(mockPostOffice);
      EasyMock.verify(queues.toArray());
      EasyMock.verify(bindings.toArray());

   }

   public void testIsStarted() throws Exception
   {
      expect(mockServer.isStarted()).andReturn(true);

      replay(mockServer);
      MessagingServerManagementImpl impl = createImpl();
      assertTrue(impl.isStarted());

      verify(mockServer);
   }

   public void testGetVersion() throws Exception
   {
      String fullVersion = randomString();
      Version version = createMock(Version.class);
      expect(version.getFullVersion()).andReturn(fullVersion);
      expect(mockServer.getVersion()).andReturn(version);

      replay(mockServer, version);
      MessagingServerManagementImpl impl = createImpl();
      assertEquals(fullVersion, impl.getVersion());

      verify(mockServer, version);
   }

   public void testGetConfiguration() throws Exception
   {
      replay(mockConfiguration);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(mockConfiguration, impl.getConfiguration());

      verify(mockConfiguration);
   }

   public void testGetQueueSettings() throws Exception
   {
      SimpleString address = randomSimpleString();
      QueueSettings settings = new QueueSettings();
      expect(mockQueueSettingsRepository.getMatch(address.toString()))
            .andReturn(settings);

      replay(mockQueueSettingsRepository);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(settings, impl.getQueueSettings(address));

      verify(mockQueueSettingsRepository);
   }

   public void testSetQueueAttributes() throws Exception
   {
      SimpleString address = randomSimpleString();
      QueueSettings settings = new QueueSettings();
      mockQueueSettingsRepository.addMatch(address.toString(), settings);

      replay(mockQueueSettingsRepository);

      MessagingServerManagementImpl impl = createImpl();
      impl.setQueueAttributes(address, settings);

      verify(mockQueueSettingsRepository);
   }

   public void testSetSecurityForAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role(randomString()));
      mockSecurityRepository.addMatch(address.toString(), roles);

      replay(mockSecurityRepository);

      MessagingServerManagementImpl impl = createImpl();
      impl.setSecurityForAddress(address, roles);

      verify(mockSecurityRepository);
   }

   public void testGetSecurityForAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      Set<Role> roles = new HashSet<Role>();
      Role role = new Role(randomString());
      roles.add(role);
      expect(mockSecurityRepository.getMatch(address.toString())).andReturn(
            roles);

      replay(mockSecurityRepository);

      MessagingServerManagementImpl impl = createImpl();
      Set<Role> matchedRoles = impl.getSecurityForAddress(address);
      assertNotNull(matchedRoles);
      assertEquals(1, matchedRoles.size());

      verify(mockSecurityRepository);
   }

   public void testRemoveSecurityForAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role(randomString()));
      mockSecurityRepository.removeMatch(address.toString());

      replay(mockSecurityRepository);

      MessagingServerManagementImpl impl = createImpl();
      impl.removeSecurityForAddress(address);

      verify(mockSecurityRepository);
   }

   public void testGetQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      Queue queue = createMock(Queue.class);
      Binding binding = createMock(Binding.class);
      expect(binding.getQueue()).andReturn(queue);
      expect(mockPostOffice.getBinding(address)).andReturn(binding);

      replay(mockPostOffice, binding, queue);

      MessagingServerManagementImpl impl = createImpl();
      assertEquals(queue, impl.getQueue(address));

      verify(mockPostOffice, binding, queue);
   }

   public void testGetQueueForUnknownAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      expect(mockPostOffice.getBinding(address)).andReturn(null);

      replay(mockPostOffice);

      MessagingServerManagementImpl impl = createImpl();
      try
      {
         impl.getQueue(address);
         fail("IllegalArgumentExcepion");
      } catch (IllegalArgumentException e)
      {
      }

      verify(mockPostOffice);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private MessagingServerManagementImpl createImpl()
   {
      return new MessagingServerManagementImpl(mockPostOffice,
            mockStorageManager, mockConfiguration, 
            mockSecurityRepository, mockQueueSettingsRepository, mockServer);
   }

   // Inner classes -------------------------------------------------

}
