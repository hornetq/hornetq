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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.impl.MessagingServerManagementImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.ConnectionManager;
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
   private ConnectionManager mockConnectionManager;
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
      mockConnectionManager = EasyMock.createNiceMock(ConnectionManager.class);
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

      beNiceOnProperties(mockPostOffice);
      
      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(null);
      
      EasyMock.expect(mockPostOffice.addBinding(destination, name, null, true, false)).andReturn(null);
      
      EasyMock.replay(mockPostOffice);
      
      MessagingServerManagementImpl impl = createImpl();
      
      impl.createQueue(destination, name);
      
      EasyMock.verify(mockPostOffice);

      EasyMock.reset(mockPostOffice);
      
      beNiceOnProperties(mockPostOffice);
      EasyMock.expect(mockPostOffice.getBinding(name)).andReturn(EasyMock.createNiceMock(Binding.class));
      
      EasyMock.replay(mockPostOffice);

      impl.createQueue(destination, name);
      
      EasyMock.verify(mockPostOffice);
      
   }

   public void testConnectionCount() throws Exception
   {
      
      mockConnectionManager = EasyMock.createMock(ConnectionManager.class);
      EasyMock.expect(mockConnectionManager.size()).andReturn(123);
      
      beNiceOnProperties(mockConnectionManager, "size");
      
      EasyMock.replay(mockConnectionManager);
      
      MessagingServerManagementImpl impl = createImpl();
      
      assertEquals(impl.getConnectionCount(), 123);
      
   }
   
   public void testDestroyQueue() throws Exception
   {
      SimpleString name = RandomUtil.randomSimpleString();
      
      mockPostOffice = EasyMock.createMock(PostOffice.class);
      beNiceOnProperties(mockPostOffice);
      
      Binding binding = EasyMock.createMock(Binding.class);
      beNiceOnProperties(binding, "queue");
      
      Queue queue = EasyMock.createMock(Queue.class);
      beNiceOnProperties(queue, "name");

      
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
      beNiceOnProperties(mockPostOffice);
      
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
      beNiceOnProperties(mockPostOffice);
      
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
         beNiceOnProperties(queue);
         queues.add(queue);
         
         Binding binding = EasyMock.createMock(Binding.class);
         beNiceOnProperties(binding, "queue");
         bindings.add(binding);
         
         EasyMock.expect(binding.getQueue()).andReturn(queue);
         
         queue.deleteAllReferences(mockStorageManager);
      }
      
      mockPostOffice = EasyMock.createMock(PostOffice.class);
      beNiceOnProperties(mockPostOffice);
      
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
         beNiceOnProperties(queue);
         queues.add(queue);
         
         Binding binding = EasyMock.createMock(Binding.class);
         beNiceOnProperties(binding, "queue");
         
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

   /** This won't really work on Strict Mocks as it will require ordering. So make sure to only use this on regular Mocks (without ordering). */
   private <T> void beNiceOnProperties(T mock, String ... ignores) throws Exception
   {
      BeanInfo info;
      info = Introspector.getBeanInfo(mock.getClass());
      
      for (PropertyDescriptor descr: info.getPropertyDescriptors())
      {
         
         // Bean Introspector will consider getClass as a property, and we need to ignore it
         if (descr.getName().equals("class"))
         {
            continue;
         }
         
         boolean ignore = false;
         
         
         
         for (String toignore: ignores)
         {
            if (descr.getName().equals(toignore) || descr.getReadMethod().getName().equals(toignore))
            {
               ignore = true;
               break;
            }
         }
         
         if (!ignore)
         {
            try
            {
               descr.getReadMethod().invoke(mock);
               
               if (descr.getReadMethod().getReturnType().equals(Boolean.class) ||
                     descr.getReadMethod().getReturnType().equals(Boolean.TYPE))
               {
                  EasyMock.expectLastCall().andReturn(false).anyTimes();
               }
               else if (descr.getReadMethod().getReturnType().equals(Integer.class) ||
                     descr.getReadMethod().getReturnType().equals(Integer.TYPE))
               {
                  EasyMock.expectLastCall().andReturn(0).anyTimes();
               }
               else if (descr.getReadMethod().getReturnType().equals(Long.class) ||
                     descr.getReadMethod().getReturnType().equals(Long.TYPE))
               {
                  EasyMock.expectLastCall().andReturn(0l).anyTimes();
               }
               else if (descr.getReadMethod().getReturnType().equals(Double.class) ||
                     descr.getReadMethod().getReturnType().equals(Double.TYPE))
               {
                  EasyMock.expectLastCall().andReturn((double)0).anyTimes();
               }
               else if (descr.getReadMethod().getReturnType().equals(Float.class) ||
                     descr.getReadMethod().getReturnType().equals(Float.TYPE))
               {
                  EasyMock.expectLastCall().andReturn((float)0).anyTimes();
               }
               else
               {
                  EasyMock.expectLastCall().andReturn(null).anyTimes();
               }
               
            }
            catch (Exception ignored)
            {
               ignored.printStackTrace();
            }
         }
      }
      
      
   }
   
   private MessagingServerManagementImpl createImpl()
   {
      return new MessagingServerManagementImpl(mockPostOffice,
            mockStorageManager, mockConfiguration, mockConnectionManager,
            mockSecurityRepository, mockQueueSettingsRepository, mockServer);
   }

   // Inner classes -------------------------------------------------
   
}
