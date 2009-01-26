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

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.easymock.classextension.EasyMock.createMock;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.management.impl.TopicControl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TopicControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
 
      replay(postOffice, storageManager);

      TopicControl control = new TopicControl(topic, jndiBinding, postOffice);
      assertEquals(name, control.getName());

      verify(postOffice, storageManager);
   }

   public void testGetAddress() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      replay(postOffice, storageManager);

      TopicControl control = new TopicControl(topic, jndiBinding, postOffice);
      assertEquals(topic.getAddress(), control.getAddress());

      verify(postOffice, storageManager);
   }

   public void testGetJNDIBinding() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      replay(postOffice, storageManager);

      TopicControl control = new TopicControl(topic, jndiBinding, postOffice);
      assertEquals(jndiBinding, control.getJNDIBinding());

      verify(postOffice, storageManager);
   }

   public void testIsTemporary() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      replay(postOffice, storageManager);

      TopicControl control = new TopicControl(topic, jndiBinding, postOffice);
      assertEquals(topic.isTemporary(), control.isTemporary());

      verify(postOffice, storageManager);
   }

   // public void testGetMessageCount() throws Exception
   // {
   // String jndiBinding = randomString();
   // String name = randomString();
   //
   // int countForNonDurableQueue = randomInt();
   // int countForDurableQueue_1 = randomInt();
   // int countForDurableQueue_2 = randomInt();
   //
   // JBossTopic topic = new JBossTopic(name);
   // PostOffice postOffice = createMock(PostOffice.class);
   // StorageManager storageManager = createMock(StorageManager.class);
   //
   // Queue nonDurableQueue = createMock(Queue.class);
   // expect(nonDurableQueue.isDurable()).andStubReturn(false);
   // expect(nonDurableQueue.getMessageCount()).andStubReturn(
   // countForNonDurableQueue);
   // Binding bindingForNonDurableQueue = createMock(Binding.class);
   // expect(bindingForNonDurableQueue.getQueue()).andStubReturn(
   // nonDurableQueue);
   //
   // Queue durableQueue_1 = createMock(Queue.class);
   // expect(durableQueue_1.isDurable()).andStubReturn(true);
   // expect(durableQueue_1.getMessageCount()).andStubReturn(
   // countForDurableQueue_1);
   // Binding bindingForDurableQueue_1 = createMock(Binding.class);
   // expect(bindingForDurableQueue_1.getQueue()).andStubReturn(durableQueue_1);
   //
   // Queue durableQueue_2 = createMock(Queue.class);
   // expect(durableQueue_2.isDurable()).andStubReturn(true);
   // expect(durableQueue_2.getMessageCount()).andStubReturn(
   // countForDurableQueue_2);
   // Binding bindingForDurableQueue_2 = createMock(Binding.class);
   // expect(bindingForDurableQueue_2.getQueue()).andStubReturn(durableQueue_2);
   //
   // Bindings bindings = new BindingsImpl();
   // bindings.addBinding(bindingForNonDurableQueue);
   // bindings.addBinding(bindingForDurableQueue_1);
   // bindings.addBinding(bindingForDurableQueue_2);
   // expect(postOffice.getBindingsForAddress(topic.getSimpleAddress()))
   // .andStubReturn(bindings);
   // replay(postOffice, storageManager, bindingForNonDurableQueue,
   // nonDurableQueue, bindingForDurableQueue_1, durableQueue_1,
   // bindingForDurableQueue_2, durableQueue_2);
   //
   // TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
   // storageManager);
   // assertEquals(countForNonDurableQueue + countForDurableQueue_1
   // + countForDurableQueue_2, control.getMessageCount());
   // assertEquals(countForDurableQueue_1 + countForDurableQueue_2, control
   // .getDurableMessagesCount());
   // assertEquals(countForNonDurableQueue, control.getNonDurableMessagesCount());
   //
   // verify(postOffice, storageManager, bindingForNonDurableQueue,
   // nonDurableQueue, bindingForDurableQueue_1, durableQueue_1,
   // bindingForDurableQueue_2, durableQueue_2);
   // }
   //
   // public void testGetSubcriptionsCount() throws Exception
   // {
   // String jndiBinding = randomString();
   // String name = randomString();
   //
   // JBossTopic topic = new JBossTopic(name);
   // PostOffice postOffice = createMock(PostOffice.class);
   // StorageManager storageManager = createMock(StorageManager.class);
   //
   // Queue nonDurableQueue = createMock(Queue.class);
   // expect(nonDurableQueue.isDurable()).andStubReturn(false);
   // Binding bindingForNonDurableQueue = createMock(Binding.class);
   // expect(bindingForNonDurableQueue.getQueue()).andStubReturn(
   // nonDurableQueue);
   //
   // Queue durableQueue_1 = createMock(Queue.class);
   // expect(durableQueue_1.isDurable()).andStubReturn(true);
   // Binding bindingForDurableQueue_1 = createMock(Binding.class);
   // expect(bindingForDurableQueue_1.getQueue()).andStubReturn(durableQueue_1);
   //
   // Queue durableQueue_2 = createMock(Queue.class);
   // expect(durableQueue_2.isDurable()).andStubReturn(true);
   // Binding bindingForDurableQueue_2 = createMock(Binding.class);
   // expect(bindingForDurableQueue_2.getQueue()).andStubReturn(durableQueue_2);
   //
   // List<Binding> bindings = new ArrayList<Binding>();
   // bindings.add(bindingForNonDurableQueue);
   // bindings.add(bindingForDurableQueue_1);
   // bindings.add(bindingForDurableQueue_2);
   // expect(postOffice.getBindingsForAddress(topic.getSimpleAddress()))
   // .andStubReturn(bindings);
   // replay(postOffice, storageManager, bindingForNonDurableQueue,
   // nonDurableQueue, bindingForDurableQueue_1, durableQueue_1,
   // bindingForDurableQueue_2, durableQueue_2);
   //
   // TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
   // storageManager);
   // assertEquals(3, control.getSubcriptionsCount());
   // assertEquals(2, control.getDurableSubcriptionsCount());
   // assertEquals(1, control.getNonDurableSubcriptionsCount());
   //
   // verify(postOffice, storageManager, bindingForNonDurableQueue,
   // nonDurableQueue, bindingForDurableQueue_1, durableQueue_1,
   // bindingForDurableQueue_2, durableQueue_2);
   // }
   //
   // public void testRemoveAllMessages() throws Exception
   // {
   // String jndiBinding = randomString();
   // String name = randomString();
   // int removedMessagesFromQueue1 = randomPositiveInt();
   // int removedMessagesFromQueue2 = randomPositiveInt();
   //
   // JBossTopic topic = new JBossTopic(name);
   // PostOffice postOffice = createMock(PostOffice.class);
   // StorageManager storageManager = createMock(StorageManager.class);
   //
   // Queue queue_1 = createMock(Queue.class);
   // Binding bindingforQueue_1 = createMock(Binding.class);
   // expect(bindingforQueue_1.getQueue()).andStubReturn(queue_1);
   //
   // Queue queue_2 = createMock(Queue.class);
   // Binding bindingForQueue_2 = createMock(Binding.class);
   // expect(bindingForQueue_2.getQueue()).andStubReturn(queue_2);
   //
   // List<Binding> bindings = new ArrayList<Binding>();
   // bindings.add(bindingforQueue_1);
   // bindings.add(bindingForQueue_2);
   // expect(postOffice.getBindingsForAddress(topic.getSimpleAddress()))
   // .andStubReturn(bindings);
   // expect(queue_1.deleteAllReferences(storageManager)).andReturn(removedMessagesFromQueue1);
   // expect(queue_2.deleteAllReferences(storageManager)).andReturn(removedMessagesFromQueue2);
   //
   // replay(postOffice, storageManager, bindingforQueue_1, queue_1,
   // bindingForQueue_2, queue_2);
   //
   // TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
   // storageManager);
   // assertEquals(removedMessagesFromQueue1 + removedMessagesFromQueue2, control.removeAllMessages());
   //
   // verify(postOffice, storageManager, bindingforQueue_1, queue_1,
   // bindingForQueue_2, queue_2);
   // }
   //
   // public void testListSubscriptionInfos() throws Exception
   // {
   // String jndiBinding = randomString();
   // String name = randomString();
   //
   // JBossTopic topic = new JBossTopic(name);
   // PostOffice postOffice = createMock(PostOffice.class);
   // StorageManager storageManager = createMock(StorageManager.class);
   //
   // Queue durableQueue = createMock(Queue.class);
   // expect(durableQueue.getName()).andStubReturn(
   // JBossTopic.createAddressFromName(randomString()));
   // expect(durableQueue.getFilter()).andStubReturn(null);
   // expect(durableQueue.isDurable()).andStubReturn(true);
   // expect(durableQueue.getMessageCount()).andStubReturn(randomInt());
   // Binding bindingForDurableQueue = createMock(Binding.class);
   // expect(bindingForDurableQueue.getQueue()).andStubReturn(durableQueue);
   //
   // Queue nonDurableQueue = createMock(Queue.class);
   // expect(nonDurableQueue.getName()).andStubReturn(
   // JBossTopic.createAddressFromName(randomString()));
   // expect(nonDurableQueue.getFilter()).andStubReturn(null);
   // expect(nonDurableQueue.isDurable()).andStubReturn(false);
   // expect(nonDurableQueue.getMessageCount()).andStubReturn(randomInt());
   // Binding bindingForNonDurableQueue = createMock(Binding.class);
   // expect(bindingForNonDurableQueue.getQueue()).andStubReturn(nonDurableQueue);
   // List<Binding> bindings = new ArrayList<Binding>();
   // bindings.add(bindingForDurableQueue);
   // bindings.add(bindingForNonDurableQueue);
   // expect(postOffice.getBindingsForAddress(topic.getSimpleAddress()))
   // .andStubReturn(bindings);
   // replay(postOffice, storageManager, bindingForDurableQueue, durableQueue,
   // bindingForNonDurableQueue, nonDurableQueue);
   //
   // TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
   // storageManager);
   // SubscriptionInfo[] infos = control.listAllSubscriptionInfos();
   // assertEquals(2, infos.length);
   // infos = control.listDurableSubscriptionInfos();
   // assertEquals(1, infos.length);
   // assertEquals(durableQueue.getName().toString(), infos[0].getQueueName());
   // infos = control.listNonDurableSubscriptionInfos();
   // assertEquals(1, infos.length);
   // assertEquals(nonDurableQueue.getName().toString(), infos[0].getQueueName());
   //
   // verify(postOffice, storageManager, bindingForDurableQueue, durableQueue,
   // bindingForNonDurableQueue, nonDurableQueue);
   // }
   //
   // public void testListSubscriptions() throws Exception
   // {
   // String jndiBinding = randomString();
   // String name = randomString();
   //
   //      
   // QueueSettings settings = new QueueSettings();
   //
   // JBossTopic topic = new JBossTopic(name);
   // PostOffice postOffice = createMock(PostOffice.class);
   // StorageManager storageManager = createMock(StorageManager.class);
   //
   // Queue durableQueue = createMock(Queue.class);
   // expect(durableQueue.getName()).andStubReturn(
   // JBossTopic.createAddressFromName(randomString()));
   // expect(durableQueue.getFilter()).andStubReturn(null);
   // expect(durableQueue.isDurable()).andStubReturn(true);
   // expect(durableQueue.getMessageCount()).andStubReturn(randomInt());
   // Binding bindingForDurableQueue = createMock(Binding.class);
   // expect(bindingForDurableQueue.getQueue()).andStubReturn(durableQueue);
   //
   // Queue nonDurableQueue = createMock(Queue.class);
   // expect(nonDurableQueue.getName()).andStubReturn(
   // JBossTopic.createAddressFromName(randomString()));
   // expect(nonDurableQueue.getFilter()).andStubReturn(null);
   // expect(nonDurableQueue.isDurable()).andStubReturn(false);
   // expect(nonDurableQueue.getMessageCount()).andStubReturn(randomInt());
   // Binding bindingForNonDurableQueue = createMock(Binding.class);
   // expect(bindingForNonDurableQueue.getQueue()).andStubReturn(nonDurableQueue);
   //
   // List<Binding> bindings = new ArrayList<Binding>();
   // bindings.add(bindingForDurableQueue);
   // bindings.add(bindingForNonDurableQueue);
   // expect(postOffice.getBindingsForAddress(topic.getSimpleAddress()))
   // .andStubReturn(bindings);
   // replay(postOffice, storageManager, bindingForDurableQueue, durableQueue,
   // bindingForNonDurableQueue, nonDurableQueue);
   //
   // TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
   // storageManager);
   // TabularData data = control.listAllSubscriptions();
   // assertEquals(2, data.size());
   // data = control.listDurableSubscriptions();
   // assertEquals(1, data.size());
   // CompositeData info = data.get(new String[] { durableQueue.getName()
   // .toString() });
   // assertNotNull(info);
   // data = control.listNonDurableSubscriptions();
   // assertEquals(1, data.size());
   // info = data.get(new String[] { nonDurableQueue.getName().toString() });
   // assertNotNull(info);
   //
   // verify(postOffice, storageManager, bindingForDurableQueue, durableQueue,
   // bindingForNonDurableQueue, nonDurableQueue);
   // }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
