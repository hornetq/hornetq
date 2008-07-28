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

import java.util.ArrayList;
import java.util.Set;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
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
      mockSecurityRepository = EasyMock.createNiceMock(HierarchicalRepository.class);
      mockQueueSettingsRepository = EasyMock.createNiceMock(HierarchicalRepository.class);
      mockServer = EasyMock.createNiceMock(MessagingServer.class);
      
   }

   public void testcreateQueue() throws Exception
   {
      SimpleString destination = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      mockPostOffice = EasyMock.createMock(PostOffice.class);

      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(null);
      
      EasyMock.expect(mockPostOffice.addBinding(destination, name, null, true, false)).andReturn(null);
      
      EasyMock.replay(mockPostOffice);
      
      MessagingServerManagementImpl impl = createImpl();
      
      impl.createQueue(destination, name);
      
      EasyMock.verify(mockPostOffice);

      EasyMock.reset(mockPostOffice);
      
      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(EasyMock.createNiceMock(Binding.class));
      
      EasyMock.replay(mockPostOffice);

      impl.createQueue(destination, name);
      
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
      
      EasyMock.expect(mockPostOffice.addDestination(address, false)).andReturn(false);
      
      EasyMock.replay(mockPostOffice);
      
      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.addDestination(address));
      
      EasyMock.reset(mockPostOffice);
      
      EasyMock.expect(mockPostOffice.addDestination(address, false)).andReturn(true);
      
      EasyMock.replay(mockPostOffice);

      assertTrue(impl.addDestination(address));
      
      EasyMock.verify(mockPostOffice);
      
   }
   
   public void testRemoveDestination() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      
      mockPostOffice = EasyMock.createMock(PostOffice.class);
      
      EasyMock.expect(mockPostOffice.removeDestination(address, false)).andReturn(false);
      
      EasyMock.replay(mockPostOffice);
      
      MessagingServerManagementImpl impl = createImpl();
      assertFalse(impl.removeDestination(address));
      
      EasyMock.reset(mockPostOffice);
      
      EasyMock.expect(mockPostOffice.removeDestination(address, false)).andReturn(true);
      
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
      
      EasyMock.expect(mockPostOffice.getBindingsForAddress(address)).andReturn(bindings);

      EasyMock.replay(mockPostOffice);
      
      EasyMock.replay(queues.toArray());
      
      EasyMock.replay(bindings.toArray());
      
      MessagingServerManagementImpl impl = createImpl();
      
      impl.removeAllMessagesForAddress(address);

      EasyMock.verify(mockPostOffice);
      
      EasyMock.verify(queues.toArray());
      
      EasyMock.verify(bindings.toArray());
      
   
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
      
      EasyMock.expect(mockPostOffice.getBindingsForAddress(address)).andReturn(bindings);
      
      EasyMock.replay(mockPostOffice);
      EasyMock.replay(queues.toArray());
      EasyMock.replay(bindings.toArray());
      
      MessagingServerManagementImpl impl = createImpl();
      assertEquals(numberOfQueues, impl.getQueuesForAddress(address).size());

      EasyMock.verify(mockPostOffice);
      EasyMock.verify(queues.toArray());
      EasyMock.verify(bindings.toArray());
      
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
