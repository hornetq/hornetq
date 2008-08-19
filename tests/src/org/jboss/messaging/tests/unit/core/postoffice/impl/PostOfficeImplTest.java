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

package org.jboss.messaging.tests.unit.core.postoffice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * A PostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class PostOfficeImplTest extends UnitTestCase
{
   private QueueFactory queueFactory = new FakeQueueFactory();

   public void testPostOfficeStart() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.replay(pm, qf);
      postOffice.start();
      EasyMock.verify(pm, qf);
      assertTrue(postOffice.isStarted());
   }

   public void testPostOfficeStartAndStop() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.replay(pm, qf);
      postOffice.start();
      postOffice.stop();
      EasyMock.verify(pm, qf);
      assertFalse(postOffice.isStarted());
   }

   public void testPostOfficeStartedAndBindingLoaded() throws Exception
   {
      Binding binding = EasyMock.createStrictMock(Binding.class);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      bindingArrayList.add(binding);

      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);

      SimpleString address1 = new SimpleString("testAddress1");
      EasyMock.expect(binding.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      SimpleString queueName = new SimpleString("testQueueName1");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setFlowController(null);
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, null));
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf, binding, queue);

      postOffice.start();

      EasyMock.verify(pm, qf, binding, queue);

      assertTrue(postOffice.isStarted());
      assertEquals(postOffice.getBinding(queueName), binding);
      assertEquals(postOffice.getBindingsForAddress(address1).size(), 1);
   }

   public void testPostOfficeStartedAndBindingsLoadedDifferentAddress() throws Exception
   {
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();

      Binding[] bindings = new Binding[100];
      Queue[] queues = new Queue[100];
      SimpleString[] addresses = new SimpleString[100];
      SimpleString[] queueNames = new SimpleString[100];
      for (int i = 0; i < 100; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         addresses[i] = new SimpleString("testAddress" + i);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(addresses[i]);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController(null);
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
      }


      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, null));
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();

      EasyMock.verify(pm, qf);

      assertTrue(postOffice.isStarted());
      for (int i = 0; i < 100; i++)
      {
         assertEquals(postOffice.getBinding(queueNames[i]), bindings[i]);
         assertEquals(postOffice.getBindingsForAddress(addresses[i]).size(), 1);
      }
   }

   public void testPostOfficeStartedAndTwoBindingSameLoadedThrowsException() throws Exception
   {
      Binding binding = EasyMock.createStrictMock(Binding.class);
      Binding binding2 = EasyMock.createStrictMock(Binding.class);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      bindingArrayList.add(binding);
      bindingArrayList.add(binding2);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      SimpleString address1 = new SimpleString("testAddress1");
      EasyMock.expect(binding.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      EasyMock.expect(binding2.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding2.getQueue()).andStubReturn(queue);
      SimpleString queueName = new SimpleString("testQueueName1");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setFlowController(null);
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, null));

      EasyMock.replay(pm, qf, binding, binding2, queue);

      try
      {
         postOffice.start();
         fail("IllegalStateException");
      }
      catch (IllegalStateException e)
      {
      }

      EasyMock.verify(pm, qf, binding, binding2, queue);

      assertFalse(postOffice.isStarted());
   }

   public void testPostOfficeStartedAndBindingsLoadedSameAddress() throws Exception
   {
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();

      Binding[] bindings = new Binding[1000];
      Queue[] queues = new Queue[1000];
      SimpleString address = new SimpleString("testAddress");
      SimpleString[] queueNames = new SimpleString[1000];
      for (int i = 0; i < 1000; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(address);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController(null);
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
      }


      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, null));
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();

      EasyMock.verify(pm, qf);

      assertTrue(postOffice.isStarted());
      for (int i = 0; i < 1000; i++)
      {
         assertEquals(postOffice.getBinding(queueNames[i]), bindings[i]);
         assertEquals(postOffice.getBindingsForAddress(address).size(), 1000);
      }
   }

   public void testPostOfficeStartedAndBindingLoadedAndDestination() throws Exception
   {
      Binding binding = EasyMock.createStrictMock(Binding.class);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      bindingArrayList.add(binding);
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      SimpleString address1 = new SimpleString("testAddress1");
      dests.add(address1);
      EasyMock.expect(binding.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      SimpleString queueName = new SimpleString("testQueueName1");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, dests));
      EasyMock.expect(pm.addDestination(address1)).andReturn(true);
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf, binding, queue);

      postOffice.start();

      EasyMock.verify(pm, qf, binding, queue);

      assertTrue(postOffice.isStarted());
      assertEquals(postOffice.getBinding(queueName), binding);
      assertEquals(postOffice.getBindingsForAddress(address1).size(), 1);
      assertTrue(postOffice.containsDestination(address1));
   }

   public void testPostOfficeStartedAndBindingLoadedAndDestinations() throws Exception
   {

      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[100];
      Queue[] queues = new Queue[100];
      SimpleString[] addresses = new SimpleString[100];
      SimpleString[] queueNames = new SimpleString[100];
      for (int i = 0; i < 100; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         addresses[i] = new SimpleString("testAddress" + i);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(addresses[i]);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController((FlowController) EasyMock.anyObject());
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
         dests.add(addresses[i]);
      }


      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, dests));
      for (int i = 0; i < 100; i++)
      {
         EasyMock.expect(pm.addDestination(addresses[i])).andReturn(true);
      }
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();

      EasyMock.verify(pm, qf);

      assertTrue(postOffice.isStarted());
      for (int i = 0; i < 100; i++)
      {
         assertEquals(postOffice.getBinding(queueNames[i]), bindings[i]);
         assertEquals(postOffice.getBindingsForAddress(addresses[i]).size(), 1);
         assertTrue(postOffice.containsDestination(addresses[i]));
      }
   }

   public void testPostOfficeStartedAndStoppedAndBindingLoadedAndDestinations() throws Exception
   {

      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[100];
      Queue[] queues = new Queue[100];
      SimpleString[] addresses = new SimpleString[100];
      SimpleString[] queueNames = new SimpleString[100];
      for (int i = 0; i < 100; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         addresses[i] = new SimpleString("testAddress" + i);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(addresses[i]);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController((FlowController) EasyMock.anyObject());
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
         dests.add(addresses[i]);
      }


      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, dests));
      for (int i = 0; i < 100; i++)
      {
         EasyMock.expect(pm.addDestination(addresses[i])).andReturn(true);
      }
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();
      postOffice.stop();
      EasyMock.verify(pm, qf);

      assertFalse(postOffice.isStarted());
      for (int i = 0; i < 100; i++)
      {
         assertNull(postOffice.getBinding(queueNames[i]));
         assertEquals(postOffice.getBindingsForAddress(addresses[i]).size(), 0);
         assertFalse(postOffice.containsDestination(addresses[i]));
      }
   }

   public void testPostOfficeFlowControllersCreateds() throws Exception
   {

      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[100];
      Queue[] queues = new Queue[100];
      SimpleString[] addresses = new SimpleString[100];
      SimpleString[] queueNames = new SimpleString[100];
      for (int i = 0; i < 100; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         addresses[i] = new SimpleString("testAddress" + i);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(addresses[i]);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController((FlowController) EasyMock.anyObject());
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
         dests.add(addresses[i]);
      }


      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, dests));
      for (int i = 0; i < 100; i++)
      {
         EasyMock.expect(pm.addDestination(addresses[i])).andReturn(true);
      }
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();

      EasyMock.verify(pm, qf);

      assertTrue(postOffice.isStarted());
      for (int i = 0; i < 100; i++)
      {
         FlowController flowController = postOffice.getFlowController(addresses[i]);
         assertNotNull(flowController);
      }
   }

   public void testListDestinations() throws Exception
   {

      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[100];
      Queue[] queues = new Queue[100];
      SimpleString[] addresses = new SimpleString[100];
      SimpleString[] queueNames = new SimpleString[100];
      for (int i = 0; i < 100; i++)
      {
         bindings[i] = EasyMock.createStrictMock(Binding.class);
         bindingArrayList.add(bindings[i]);
         queues[i] = EasyMock.createStrictMock(Queue.class);
         addresses[i] = new SimpleString("testAddress" + i);
         queueNames[i] = new SimpleString("testQueueName" + i);
         EasyMock.expect(bindings[i].getAddress()).andStubReturn(addresses[i]);
         EasyMock.expect(bindings[i].getQueue()).andStubReturn(queues[i]);
         EasyMock.expect(queues[i].getName()).andStubReturn(queueNames[i]);
         queues[i].setFlowController((FlowController) EasyMock.anyObject());
         EasyMock.expect(queues[i].getPersistenceID()).andStubReturn(i + 1);
         EasyMock.replay(bindings[i], queues[i]);
         dests.add(addresses[i]);
      }


      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList, dests));
      for (int i = 0; i < 100; i++)
      {
         EasyMock.expect(pm.addDestination(addresses[i])).andReturn(true);
      }
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();
      Set<SimpleString> allDests = postOffice.listAllDestinations();
      EasyMock.verify(pm, qf);

      for (SimpleString dest : allDests)
      {
         assertTrue(dests.remove(dest));
      }
      assertTrue(dests.size() == 0);
   }

   public void testAddQueue() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice po = new PostOfficeImpl(pm, qf, ms, false);

      final long id = 324;
      final SimpleString name = new SimpleString("wibb22");
      final Filter filter = EasyMock.createMock(Filter.class);
      final boolean durable = true;

      Queue queue = queueFactory.createQueue(id, name, filter, durable);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable)).andReturn(queue);

      final SimpleString condition = new SimpleString("queue.wibble");

      Binding expected = new BindingImpl(condition, queue);

      pm.addBinding(EasyMock.eq(expected));

      EasyMock.replay(qf, pm, filter);

      po.addBinding(condition, name, filter, durable);

      EasyMock.verify(qf, pm, filter);

      EasyMock.reset(qf, pm, filter);

      final boolean durable2 = false;

      queue = queueFactory.createQueue(id, name, filter, durable2);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable2)).andReturn(queue);

      EasyMock.replay(qf, pm, filter);

   }

   public void testRemoveQueue() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice po = new PostOfficeImpl(pm, qf, ms, false);

      final long id = 324;
      final SimpleString name = new SimpleString("wibb22");
      final Filter filter = EasyMock.createMock(Filter.class);
      final boolean durable = true;
  
      Queue queue = queueFactory.createQueue(id, name, filter, durable);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable)).andReturn(queue);

      final SimpleString condition = new SimpleString("queue.wibble");

      Binding expected = new BindingImpl(condition, queue);

      pm.addBinding(EasyMock.eq(expected));

      pm.deleteBinding(EasyMock.eq(expected));

      EasyMock.replay(qf, pm, filter);

      po.addBinding(condition, name, filter, durable);

      po.removeBinding(name);

      EasyMock.verify(qf, pm, filter);

      EasyMock.reset(qf, pm, filter);

      final boolean durable2 = false;

      queue = queueFactory.createQueue(id, name, filter, durable2);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable2)).andReturn(queue);

      EasyMock.replay(qf, pm, filter);

      po.addBinding(condition, name, filter, durable2);

      po.removeBinding(name);

      EasyMock.verify(qf, pm, filter);
   }

   public void testAddRemoveMultipleWithDifferentConditions() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = new FakeQueueFactory();
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);

      PostOffice po = new PostOfficeImpl(pm, qf, ms, false);

      final SimpleString condition1 = new SimpleString("queue.wibble");

      SimpleString squeue1 = new SimpleString("queue1");
      SimpleString squeue2 = new SimpleString("queue2");
      SimpleString squeue3 = new SimpleString("queue3");
      SimpleString squeue4 = new SimpleString("queue4");
      SimpleString squeue5 = new SimpleString("queue5");
      SimpleString squeue6 = new SimpleString("queue6");

      po.addBinding(condition1, squeue1, null, false);
      Map<SimpleString, List<Binding>> mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.addBinding(condition1, squeue2, null, false);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.addBinding(condition1, squeue3, null, false);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      List<Binding> bindings = mappings.get(condition1);
      assertNotNull(bindings);
      assertEquals(3, bindings.size());

      Binding binding1 = bindings.get(0);
      Queue queue1 = binding1.getQueue();
      assertEquals(squeue1, queue1.getName());

      Binding binding2 = bindings.get(1);
      Queue queue2 = binding2.getQueue();
      assertEquals(squeue2, queue2.getName());

      Binding binding3 = bindings.get(2);
      Queue queue3 = binding3.getQueue();
      assertEquals(squeue3, queue3.getName());

      final SimpleString condition2 = new SimpleString("queue.wibble2");

      po.addBinding(condition2, squeue4, null, false);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      po.addBinding(condition2, squeue5, null, false);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      final SimpleString condition3 = new SimpleString("topic.wibblexyz");

      po.addBinding(condition3, squeue6, null, false);
      mappings = po.getMappings();
      assertEquals(3, mappings.size());

      po.removeBinding(squeue6);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      po.removeBinding(squeue4);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      po.removeBinding(squeue5);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.removeBinding(squeue1);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.removeBinding(squeue2);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.removeBinding(squeue3);
      mappings = po.getMappings();
      assertEquals(0, mappings.size());
   }

   public void testPostOfficeAddDestination() throws Exception
   {
      SimpleString address = new SimpleString("testAddress");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.replay(pm, qf);
      postOffice.start();
      assertTrue(postOffice.addDestination(address, true));
      assertNotNull(postOffice.getFlowController(address));
      assertTrue(postOffice.containsDestination(address));
      EasyMock.verify(pm, qf);
   }

   public void testPostOfficeAddDestinations() throws Exception
   {

      SimpleString address = new SimpleString("testAddress");
      SimpleString address2 = new SimpleString("testAddress2");
      SimpleString address3 = new SimpleString("testAddress3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.expect(pm.addDestination(address2)).andReturn(true);
      EasyMock.expect(pm.addDestination(address3)).andReturn(true);
      EasyMock.replay(pm, qf);
      postOffice.start();
      assertTrue(postOffice.addDestination(address, true));
      assertTrue(postOffice.addDestination(address2, true));
      assertTrue(postOffice.addDestination(address3, true));
      assertNotNull(postOffice.getFlowController(address));
      assertNotNull(postOffice.getFlowController(address2));
      assertNotNull(postOffice.getFlowController(address3));
      assertTrue(postOffice.containsDestination(address));
      assertTrue(postOffice.containsDestination(address2));
      assertTrue(postOffice.containsDestination(address3));
      EasyMock.verify(pm, qf);
   }

   public void testPostOfficeAddAndRemoveDestination() throws Exception
   {

      SimpleString address = new SimpleString("testAddress");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.expect(pm.deleteDestination(address)).andReturn(true);
      EasyMock.replay(pm, qf);
      postOffice.start();
      assertTrue(postOffice.addDestination(address, true));
      assertTrue(postOffice.containsDestination(address));
      postOffice.removeDestination(address, true);
      assertNull(postOffice.getFlowController(address));
      assertFalse(postOffice.containsDestination(address));
      EasyMock.verify(pm, qf);
   }

   public void testPostOfficeAddAndRemoveDestinations() throws Exception
   {

      SimpleString address = new SimpleString("testAddress");
      SimpleString address2 = new SimpleString("testAddress2");
      SimpleString address3 = new SimpleString("testAddress3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.expect(pm.addDestination(address2)).andReturn(true);
      EasyMock.expect(pm.addDestination(address3)).andReturn(true);
      EasyMock.expect(pm.deleteDestination(address)).andReturn(true);
      EasyMock.expect(pm.deleteDestination(address3)).andReturn(true);
      EasyMock.replay(pm, qf);
      postOffice.start();
      assertTrue(postOffice.addDestination(address, true));
      assertTrue(postOffice.addDestination(address2, true));
      assertTrue(postOffice.addDestination(address3, true));
      assertNotNull(postOffice.getFlowController(address));
      assertNotNull(postOffice.getFlowController(address2));
      assertNotNull(postOffice.getFlowController(address3));
      assertTrue(postOffice.containsDestination(address));
      assertTrue(postOffice.containsDestination(address2));
      assertTrue(postOffice.containsDestination(address3));
      postOffice.removeDestination(address, true);
      postOffice.removeDestination(address3, true);
      assertNull(postOffice.getFlowController(address));
      assertFalse(postOffice.containsDestination(address));
      assertNotNull(postOffice.getFlowController(address2));
      assertTrue(postOffice.containsDestination(address2));
      assertNull(postOffice.getFlowController(address3));
      assertFalse(postOffice.containsDestination(address3));
      EasyMock.verify(pm, qf);
   }

  
   public void testAddDurableBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
      assertNotNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testAddDurableBindings() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      SimpleString queueName2 = new SimpleString("testQueueName2");
      SimpleString queueName3 = new SimpleString("testQueueName3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, filter, true)).andReturn(queue2);
      EasyMock.expect(qf.createQueue(-1, queueName3, filter, true)).andReturn(queue3);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      queue2.setBackup(false);
      queue2.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
      queue3.setBackup(false);
      queue3.setFlowController((FlowController) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      EasyMock.replay(pm, qf, queue, queue2, queue3);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
      postOffice.addBinding(new SimpleString("testAddress2"), queueName2, filter, true);
      postOffice.addBinding(new SimpleString("testAddress3"), queueName3, filter, true);
      assertNotNull(postOffice.getBinding(queueName));
      assertNotNull(postOffice.getBinding(queueName2));
      assertNotNull(postOffice.getBinding(queueName3));
      EasyMock.verify(pm, qf, queue, queue2, queue3);
   }

   public void testAddNonDurableBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, false);
      assertNotNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testAddNonDurableBindings() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      SimpleString queueName2 = new SimpleString("testQueueName2");
      SimpleString queueName3 = new SimpleString("testQueueName3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, filter, false)).andReturn(queue2);
      EasyMock.expect(qf.createQueue(-1, queueName3, filter, false)).andReturn(queue3);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      queue2.setBackup(false);
      queue2.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
      queue3.setBackup(false);
      queue3.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.replay(pm, qf, queue, queue2, queue3);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, false);
      postOffice.addBinding(new SimpleString("testAddress2"), queueName2, filter, false);
      postOffice.addBinding(new SimpleString("testAddress3"), queueName3, filter, false);
      assertNotNull(postOffice.getBinding(queueName));
      assertNotNull(postOffice.getBinding(queueName2));
      assertNotNull(postOffice.getBinding(queueName3));
      EasyMock.verify(pm, qf, queue, queue2, queue3);
   }

   public void testAddSameBindingThrowsException() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);      
      EasyMock.expect(queue.getName()).andStubReturn(queueName); 
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      queue.setBackup(false);
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
      try
      {
         postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      assertNotNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testAddAndRemoveDurableBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      pm.deleteBinding((Binding) EasyMock.anyObject());
      queue.setFlowController(null);
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
      postOffice.removeBinding(queueName);
      assertNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testAddAndRemoveDurableBindings() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      SimpleString queueName2 = new SimpleString("testQueueName2");
      SimpleString queueName3 = new SimpleString("testQueueName3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, true)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, filter, true)).andReturn(queue2);
      EasyMock.expect(qf.createQueue(-1, queueName3, filter, true)).andReturn(queue3);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      queue2.setBackup(false);
      queue2.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
      queue3.setBackup(false);
      queue3.setFlowController((FlowController) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      pm.addBinding((Binding) EasyMock.anyObject());
      pm.deleteBinding((Binding) EasyMock.anyObject());
      pm.deleteBinding((Binding) EasyMock.anyObject());
      EasyMock.expect(queue.isDurable()).andStubReturn(true);
      queue.setFlowController(null);
      EasyMock.expect(queue3.isDurable()).andStubReturn(true);
      queue3.setFlowController(null);
      EasyMock.replay(pm, qf, queue, queue2, queue3);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, true);
      postOffice.addBinding(new SimpleString("testAddress2"), queueName2, filter, true);
      postOffice.addBinding(new SimpleString("testAddress3"), queueName3, filter, true);
      postOffice.removeBinding(queueName);
      postOffice.removeBinding(queueName3);
      assertNull(postOffice.getBinding(queueName));
      assertNotNull(postOffice.getBinding(queueName2));
      assertNull(postOffice.getBinding(queueName3));
      EasyMock.verify(pm, qf, queue, queue2, queue3);
   }

   public void testAddAndRemoveNonDurableBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue.isDurable()).andStubReturn(false);
      queue.setFlowController(null);
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, false);
      postOffice.removeBinding(queueName);
      assertNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testAddAndRemoveNonDurableBindings() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      SimpleString queueName2 = new SimpleString("testQueueName2");
      SimpleString queueName3 = new SimpleString("testQueueName3");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, filter, false)).andReturn(queue2);
      EasyMock.expect(qf.createQueue(-1, queueName3, filter, false)).andReturn(queue3);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      queue2.setBackup(false);
      queue2.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
      queue3.setBackup(false);
      queue3.setFlowController((FlowController) EasyMock.anyObject());

      EasyMock.expect(queue.isDurable()).andStubReturn(false);
      queue.setFlowController(null);
      EasyMock.expect(queue3.isDurable()).andStubReturn(false);
      queue3.setFlowController(null);
      EasyMock.replay(pm, qf, queue, queue2, queue3);
      postOffice.start();

      postOffice.addBinding(new SimpleString("testAddress"), queueName, filter, false);
      postOffice.addBinding(new SimpleString("testAddress2"), queueName2, filter, false);
      postOffice.addBinding(new SimpleString("testAddress3"), queueName3, filter, false);
      postOffice.removeBinding(queueName);
      postOffice.removeBinding(queueName3);
      assertNull(postOffice.getBinding(queueName));
      assertNotNull(postOffice.getBinding(queueName2));
      assertNull(postOffice.getBinding(queueName3));
      EasyMock.verify(pm, qf, queue, queue2, queue3);
   }

   public void testRemoveNonExistingBindingThrowsException() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.isDurable()).andStubReturn(false);
      EasyMock.replay(pm, qf, queue);
      postOffice.start();

      try
      {
         postOffice.removeBinding(queueName);
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
      assertNull(postOffice.getBinding(queueName));
      EasyMock.verify(pm, qf, queue);
   }

   public void testPostOfficeCannotRouteThrowsException() throws Exception
   {
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(message.getDestination()).andStubReturn(new SimpleString("testtDestination"));
      EasyMock.replay(pm, qf, message);
      postOffice.start();
      try
      {
         postOffice.route(message);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         MessagingException messagingException = (MessagingException) e;
         assertEquals(MessagingException.ADDRESS_DOES_NOT_EXIST, messagingException.getCode());
      }
      EasyMock.verify(pm, qf, message);
      assertTrue(postOffice.isStarted());
   }

   public void testPostOfficeCannotRouteDoesntThrowsException() throws Exception
   {
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.expect(message.getDestination()).andStubReturn(new SimpleString("testtDestination"));
      EasyMock.replay(pm, qf, message);
      postOffice.start();
      postOffice.route(message);
      EasyMock.verify(pm, qf, message);
      assertTrue(postOffice.isStarted());
   }

   public void testPostOfficeRouteToSingleQueueNullFilter() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      SimpleString address = new SimpleString("testtDestination");
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(qf.createQueue(-1, queueName, null, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.getFilter()).andStubReturn(null);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(message.createReference(queue)).andReturn(messageReference);
      EasyMock.replay(pm, qf, message, queue, messageReference);
      postOffice.start();
      postOffice.addBinding(address, queueName, null, false);
      List<MessageReference> references = postOffice.route(message);
      EasyMock.verify(pm, qf, message, queue, messageReference);
      assertTrue(postOffice.isStarted());
      assertEquals(1, references.size());
      assertEquals(messageReference, references.get(0));
   }

   public void testPostOfficeRouteToSingleQueueValidFilter() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      Filter filter = EasyMock.createStrictMock(Filter.class);
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      SimpleString address = new SimpleString("testtDestination");
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.getFilter()).andStubReturn(filter);      
      EasyMock.expect(filter.match(message)).andReturn(true);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(message.createReference(queue)).andReturn(messageReference);
      EasyMock.replay(pm, qf, message, queue, messageReference, filter);
      postOffice.start();
      postOffice.addBinding(address, queueName, filter, false);
      List<MessageReference> references = postOffice.route(message);
      EasyMock.verify(pm, qf, message, queue, messageReference, filter);
      assertTrue(postOffice.isStarted());
      assertEquals(1, references.size());
      assertEquals(messageReference, references.get(0));
   }

   public void testPostOfficeRouteToSingleQueueInValidFilter() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      Filter filter = EasyMock.createStrictMock(Filter.class);
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      SimpleString address = new SimpleString("testtDestination");
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(qf.createQueue(-1, queueName, filter, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.getFilter()).andStubReturn(filter);
      EasyMock.expect(filter.match(message)).andReturn(false);
      queue.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.replay(pm, qf, message, queue, messageReference, filter);
      postOffice.start();
      postOffice.addBinding(address, queueName, filter, false);
      List<MessageReference> references = postOffice.route(message);
      EasyMock.verify(pm, qf, message, queue, messageReference, filter);
      assertTrue(postOffice.isStarted());
      assertEquals(0, references.size());
   }

   public void testPostOfficeRouteToMultipleQueuesNullFilter() throws Exception
   {
      SimpleString queueName = new SimpleString("testQueueName");
      SimpleString queueName2 = new SimpleString("testQueueName2");
      SimpleString queueName3 = new SimpleString("testQueueName3");
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference3 = EasyMock.createStrictMock(MessageReference.class);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      Queue queue3 = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      SimpleString address = new SimpleString("testtDestination");
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(qf.createQueue(-1, queueName, null, false)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, null, false)).andReturn(queue2);
      EasyMock.expect(qf.createQueue(-1, queueName3, null, false)).andReturn(queue3);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.getFilter()).andStubReturn(null);
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      EasyMock.expect(queue2.getFilter()).andStubReturn(null);
      EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
      EasyMock.expect(queue3.getFilter()).andStubReturn(null);
      queue.setBackup(false);
      queue2.setBackup(false);
      queue3.setBackup(false);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      queue2.setFlowController((FlowController) EasyMock.anyObject());
      queue3.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(message.createReference(queue)).andReturn(messageReference);
      EasyMock.expect(message.createReference(queue2)).andReturn(messageReference2);
      EasyMock.expect(message.createReference(queue3)).andReturn(messageReference3);
      EasyMock.replay(pm, qf, message, queue, queue2, queue3, messageReference);
      postOffice.start();
      postOffice.addBinding(address, queueName, null, false);
      postOffice.addBinding(address, queueName2, null, false);
      postOffice.addBinding(address, queueName3, null, false);
      List<MessageReference> references = postOffice.route(message);
      EasyMock.verify(pm, qf, message, queue, queue2, queue3, messageReference);
      assertTrue(postOffice.isStarted());
      assertEquals(3, references.size());
      assertEquals(messageReference, references.get(0));
      assertEquals(messageReference2, references.get(1));
      assertEquals(messageReference3, references.get(2));
   }

   public void testPostOfficeRouteToMultipleQueuesMixedFilters() throws Exception
      {
         SimpleString queueName = new SimpleString("testQueueName");
         SimpleString queueName2 = new SimpleString("testQueueName2");
         SimpleString queueName3 = new SimpleString("testQueueName3");
         Filter filter = EasyMock.createStrictMock(Filter.class);
         Filter filter2 = EasyMock.createStrictMock(Filter.class);
         ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
         MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
         MessageReference messageReference2 = EasyMock.createStrictMock(MessageReference.class);
         MessageReference messageReference3 = EasyMock.createStrictMock(MessageReference.class);
         Queue queue = EasyMock.createStrictMock(Queue.class);
         Queue queue2 = EasyMock.createStrictMock(Queue.class);
         Queue queue3 = EasyMock.createStrictMock(Queue.class);
         StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
         QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
         ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
         PostOffice postOffice = new PostOfficeImpl(pm, qf, ms, false);
         pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
         pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
         SimpleString address = new SimpleString("testtDestination");
         EasyMock.expect(message.getDestination()).andStubReturn(address);
         EasyMock.expect(qf.createQueue(-1, queueName, null, false)).andReturn(queue);
         EasyMock.expect(qf.createQueue(-1, queueName2, null, false)).andReturn(queue2);
         EasyMock.expect(qf.createQueue(-1, queueName3, null, false)).andReturn(queue3);
         EasyMock.expect(queue.getName()).andStubReturn(queueName);
         EasyMock.expect(queue.getFilter()).andStubReturn(filter);
         EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
         EasyMock.expect(queue2.getFilter()).andStubReturn(null);
         EasyMock.expect(queue3.getName()).andStubReturn(queueName3);
         EasyMock.expect(queue3.getFilter()).andStubReturn(filter2);
         EasyMock.expect(filter.match(message)).andReturn(false);
         EasyMock.expect(filter2.match(message)).andReturn(true);
         queue.setBackup(false);
         queue2.setBackup(false);
         queue3.setBackup(false);
         queue.setFlowController((FlowController) EasyMock.anyObject());
         queue2.setFlowController((FlowController) EasyMock.anyObject());
         queue3.setFlowController((FlowController) EasyMock.anyObject());
         EasyMock.expect(message.createReference(queue2)).andReturn(messageReference2);
         EasyMock.expect(message.createReference(queue3)).andReturn(messageReference3);
         EasyMock.replay(pm, qf, message, queue, queue2, queue3, messageReference, filter, filter2);
         postOffice.start();
         postOffice.addBinding(address, queueName, null, false);
         postOffice.addBinding(address, queueName2, null, false);
         postOffice.addBinding(address, queueName3, null, false);
         List<MessageReference> references = postOffice.route(message);
         EasyMock.verify(pm, qf, message, queue, queue2, queue3, messageReference, filter, filter2);
         assertTrue(postOffice.isStarted());
         assertEquals(2, references.size());
         assertEquals(messageReference2, references.get(0));
         assertEquals(messageReference3, references.get(1));
      }

   class LoadBindingsIAnswer implements IAnswer
   {
      List<Binding> bindings;
      List<SimpleString> dests;

      public LoadBindingsIAnswer(List<Binding> bindings, List<SimpleString> dests)
      {
         this.bindings = bindings;
         this.dests = dests;
      }

      public Object answer() throws Throwable
      {
         if (this.bindings != null)
         {
            List<Binding> bindings = (List<Binding>) EasyMock.getCurrentArguments()[1];
            bindings.addAll(this.bindings);
         }
         if (this.dests != null)
         {
            List<SimpleString> dests = (List<SimpleString>) EasyMock.getCurrentArguments()[2];
            dests.addAll(this.dests);
         }
         return null;
      }
   }
}
