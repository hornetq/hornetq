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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.management.SubscriberInfo;
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
      MessagingServerManagement server = createMock(MessagingServerManagement.class);

      replay(server);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(name, control.getName());

      verify(server);
   }

   public void testGetAddress() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);

      replay(server);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(topic.getAddress(), control.getAddress());

      verify(server);
   }

   public void testGetJNDIBinding() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);

      replay(server);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(jndiBinding, control.getJNDIBinding());

      verify(server);
   }

   public void testIsTemporary() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);

      replay(server);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(topic.isTemporary(), control.isTemporary());

      verify(server);
   }

   public void testGetMessageCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      int countForNonDurableQueue = randomInt();
      int countForDurableQueue_1 = randomInt();
      int countForDurableQueue_2 = randomInt();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Queue nonDurableQueue = createMock(Queue.class);
      expect(nonDurableQueue.isDurable()).andStubReturn(false);
      expect(nonDurableQueue.getMessageCount()).andStubReturn(
            countForNonDurableQueue);
      Queue durableQueue_1 = createMock(Queue.class);
      expect(durableQueue_1.isDurable()).andStubReturn(true);
      expect(durableQueue_1.getMessageCount()).andStubReturn(
            countForDurableQueue_1);
      Queue durableQueue_2 = createMock(Queue.class);
      expect(durableQueue_2.isDurable()).andStubReturn(true);
      expect(durableQueue_2.getMessageCount()).andStubReturn(
            countForDurableQueue_2);
      List<Queue> queues = new ArrayList<Queue>();
      queues.add(nonDurableQueue);
      queues.add(durableQueue_1);
      queues.add(durableQueue_2);
      expect(server.getQueuesForAddress(topic.getSimpleAddress()))
            .andStubReturn(queues);
      replay(server, nonDurableQueue, durableQueue_1, durableQueue_2);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(countForNonDurableQueue + countForDurableQueue_1
            + countForDurableQueue_2, control.getMessageCount());
      assertEquals(countForDurableQueue_1 + countForDurableQueue_2, control
            .getDurableMessageCount());
      assertEquals(countForNonDurableQueue, control.getNonDurableMessageCount());

      verify(server, nonDurableQueue, durableQueue_1, durableQueue_2);
   }

   public void testGetSubcribersCount() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Queue nonDurableQueue = createMock(Queue.class);
      expect(nonDurableQueue.isDurable()).andStubReturn(false);
      Queue durableQueue_1 = createMock(Queue.class);
      expect(durableQueue_1.isDurable()).andStubReturn(true);
      Queue durableQueue_2 = createMock(Queue.class);
      expect(durableQueue_2.isDurable()).andStubReturn(true);
      List<Queue> queues = new ArrayList<Queue>();
      queues.add(nonDurableQueue);
      queues.add(durableQueue_1);
      queues.add(durableQueue_2);
      expect(server.getQueuesForAddress(topic.getSimpleAddress()))
            .andStubReturn(queues);
      replay(server, nonDurableQueue, durableQueue_1, durableQueue_2);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      assertEquals(3, control.getSubcribersCount());
      assertEquals(2, control.getDurableSubcribersCount());
      assertEquals(1, control.getNonDurableSubcribersCount());

      verify(server, nonDurableQueue, durableQueue_1, durableQueue_2);
   }

   public void testRemoveAllMessages() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      server.removeAllMessagesForAddress(topic.getSimpleAddress());
      replay(server);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      control.removeAllMessages();

      verify(server);
   }

   public void testListSubscriberInfos() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Queue durableQueue = createMock(Queue.class);
      expect(durableQueue.getName()).andStubReturn(
            JBossTopic.createAddressFromName(randomString()));
      expect(durableQueue.getFilter()).andStubReturn(null);
      expect(durableQueue.isDurable()).andStubReturn(true);
      expect(durableQueue.getMessageCount()).andStubReturn(randomInt());
      expect(durableQueue.getMaxSizeBytes()).andStubReturn(randomInt());
      Queue nonDurableQueue = createMock(Queue.class);
      expect(nonDurableQueue.getName()).andStubReturn(
            JBossTopic.createAddressFromName(randomString()));
      expect(nonDurableQueue.getFilter()).andStubReturn(null);
      expect(nonDurableQueue.isDurable()).andStubReturn(false);
      expect(nonDurableQueue.getMessageCount()).andStubReturn(randomInt());
      expect(nonDurableQueue.getMaxSizeBytes()).andStubReturn(randomInt());
      List<Queue> queues = new ArrayList<Queue>();
      queues.add(durableQueue);
      queues.add(nonDurableQueue);
      expect(server.getQueuesForAddress(topic.getSimpleAddress()))
            .andStubReturn(queues);
      replay(server, durableQueue, nonDurableQueue);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      SubscriberInfo[] infos = control.listAllSubscriberInfos();
      assertEquals(2, infos.length);
      infos = control.listDurableSubscriberInfos();
      assertEquals(1, infos.length);
      assertEquals(durableQueue.getName().toString(), infos[0].getID());
      infos = control.listNonDurableSubscriberInfos();
      assertEquals(1, infos.length);
      assertEquals(nonDurableQueue.getName().toString(), infos[0].getID());

      verify(server, durableQueue, nonDurableQueue);
   }

   public void testListSubscribers() throws Exception
   {
      String jndiBinding = randomString();
      String name = randomString();

      JBossTopic topic = new JBossTopic(name);
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Queue durableQueue = createMock(Queue.class);
      expect(durableQueue.getName()).andStubReturn(
            JBossTopic.createAddressFromName(randomString()));
      expect(durableQueue.getFilter()).andStubReturn(null);
      expect(durableQueue.isDurable()).andStubReturn(true);
      expect(durableQueue.getMessageCount()).andStubReturn(randomInt());
      expect(durableQueue.getMaxSizeBytes()).andStubReturn(randomInt());
      Queue nonDurableQueue = createMock(Queue.class);
      expect(nonDurableQueue.getName()).andStubReturn(
            JBossTopic.createAddressFromName(randomString()));
      expect(nonDurableQueue.getFilter()).andStubReturn(null);
      expect(nonDurableQueue.isDurable()).andStubReturn(false);
      expect(nonDurableQueue.getMessageCount()).andStubReturn(randomInt());
      expect(nonDurableQueue.getMaxSizeBytes()).andStubReturn(randomInt());
      List<Queue> queues = new ArrayList<Queue>();
      queues.add(durableQueue);
      queues.add(nonDurableQueue);
      expect(server.getQueuesForAddress(topic.getSimpleAddress()))
            .andStubReturn(queues);
      replay(server, durableQueue, nonDurableQueue);

      TopicControl control = new TopicControl(topic, server, jndiBinding);
      TabularData data = control.listAllSubscribers();
      assertEquals(2, data.size());
      data = control.listDurableSubscribers();
      assertEquals(1, data.size());
      CompositeData info = data.get(new String[] { durableQueue.getName().toString() });
      assertNotNull(info);
      data = control.listNonDurableSubscribers();
      assertEquals(1, data.size());
      info = data.get(new String[] { nonDurableQueue.getName().toString() });
      assertNotNull(info);

      verify(server, durableQueue, nonDurableQueue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
