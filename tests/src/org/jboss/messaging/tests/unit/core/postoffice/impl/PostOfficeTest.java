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

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A PostOfficeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class PostOfficeTest extends UnitTestCase
{
   private QueueFactory queueFactory = new FakeQueueFactory();

   public void testPostOfficeStart() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());
      EasyMock.replay(pm, qf);
      postOffice.start();
      EasyMock.verify(pm, qf);
      assertTrue(postOffice.isStarted());
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
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
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

      Binding[] bindings = new Binding[1000];
      Queue[] queues = new Queue[1000];
      SimpleString[] addresses = new SimpleString[1000];
      SimpleString[] queueNames = new SimpleString[1000];
      for (int i = 0; i < 1000; i++)
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


      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
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

      SimpleString address1 = new SimpleString("testAddress1");
      EasyMock.expect(binding.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      EasyMock.expect(binding2.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding2.getQueue()).andStubReturn(queue);
      SimpleString queueName = new SimpleString("testQueueName1");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setFlowController(null);
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new LoadBindingsIAnswer(bindingArrayList,null));

      EasyMock.replay(pm, qf, binding, binding2, queue);

      try
      {
         postOffice.start();
      }
      catch (IllegalStateException e)
      {
         e.printStackTrace();
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


      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
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

      SimpleString address1 = new SimpleString("testAddress1");
      dests.add(address1);
      EasyMock.expect(binding.getAddress()).andStubReturn(address1);
      EasyMock.expect(binding.getQueue()).andStubReturn(queue);
      SimpleString queueName = new SimpleString("testQueueName1");
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      queue.setFlowController((FlowController) EasyMock.anyObject());
      EasyMock.expect(queue.getPersistenceID()).andStubReturn(1);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
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
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[1000];
      Queue[] queues = new Queue[1000];
      SimpleString[] addresses = new SimpleString[1000];
      SimpleString[] queueNames = new SimpleString[1000];
      for (int i = 0; i < 1000; i++)
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
      for (int i = 0; i < 1000; i++)
      {
         EasyMock.expect(pm.addDestination(addresses[i])).andReturn(true);
      }
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject());

      EasyMock.replay(pm, qf);

      postOffice.start();

      EasyMock.verify(pm, qf);

      assertTrue(postOffice.isStarted());
      for (int i = 0; i < 1000; i++)
      {
         assertEquals(postOffice.getBinding(queueNames[i]), bindings[i]);
         assertEquals(postOffice.getBindingsForAddress(addresses[i]).size(), 1);
         assertTrue(postOffice.containsDestination(addresses[i]));
      }
   }

   public void testListDestinations() throws Exception
   {

      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      PostOffice postOffice = new PostOfficeImpl(pm, qf, true);
      ArrayList<Binding> bindingArrayList = new ArrayList<Binding>();
      List<SimpleString> dests = new ArrayList<SimpleString>();
      Binding[] bindings = new Binding[1000];
      Queue[] queues = new Queue[1000];
      SimpleString[] addresses = new SimpleString[1000];
      SimpleString[] queueNames = new SimpleString[1000];
      for (int i = 0; i < 1000; i++)
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
      for (int i = 0; i < 1000; i++)
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

      PostOffice po = new PostOfficeImpl(pm, qf, false);

      final long id = 324;
      final SimpleString name = new SimpleString("wibb22");
      final Filter filter = new FakeFilter();
      final boolean durable = true;
      final boolean temporary = true;

      Queue queue = queueFactory.createQueue(id, name, filter, durable, temporary);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable, temporary)).andReturn(queue);

      final SimpleString condition = new SimpleString("queue.wibble");

      Binding expected = new BindingImpl(condition, queue);

      pm.addBinding(EasyMock.eq(expected));

      EasyMock.replay(qf);

      EasyMock.replay(pm);

      po.addBinding(condition, name, filter, durable, temporary);

      EasyMock.verify(qf);

      EasyMock.verify(pm);

      EasyMock.reset(qf);

      EasyMock.reset(pm);

      final boolean durable2 = false;

      queue = queueFactory.createQueue(id, name, filter, durable2, temporary);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable2, temporary)).andReturn(queue);

      EasyMock.replay(qf);

      EasyMock.replay(pm);
   }

   public void testRemoveQueue() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);

      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);

      PostOffice po = new PostOfficeImpl(pm, qf, false);

      final long id = 324;
      final SimpleString name = new SimpleString("wibb22");
      final Filter filter = new FakeFilter();
      final boolean durable = true;
      final boolean temporary = true;

      Queue queue = queueFactory.createQueue(id, name, filter, durable, temporary);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable, temporary)).andReturn(queue);

      final SimpleString condition = new SimpleString("queue.wibble");

      Binding expected = new BindingImpl(condition, queue);

      pm.addBinding(EasyMock.eq(expected));

      pm.deleteBinding(EasyMock.eq(expected));

      EasyMock.replay(qf);

      EasyMock.replay(pm);

      po.addBinding(condition, name, filter, durable, temporary);

      po.removeBinding(name);

      EasyMock.verify(qf);

      EasyMock.verify(pm);

      EasyMock.reset(qf);

      EasyMock.reset(pm);

      final boolean durable2 = false;

      queue = queueFactory.createQueue(id, name, filter, durable2, temporary);

      EasyMock.expect(qf.createQueue(-1, name, filter, durable2, temporary)).andReturn(queue);

      EasyMock.replay(qf);

      EasyMock.replay(pm);

      po.addBinding(condition, name, filter, durable2, temporary);

      po.removeBinding(name);

      EasyMock.verify(qf);

      EasyMock.verify(pm);
   }

   public void testAddRemoveMultipleWithDifferentConditions() throws Exception
   {
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);

      QueueFactory qf = new FakeQueueFactory();

      PostOffice po = new PostOfficeImpl(pm, qf, false);

      final SimpleString condition1 = new SimpleString("queue.wibble");

      SimpleString squeue1 = new SimpleString("queue1");
      SimpleString squeue2 = new SimpleString("queue2");
      SimpleString squeue3 = new SimpleString("queue3");
      SimpleString squeue4 = new SimpleString("queue4");
      SimpleString squeue5 = new SimpleString("queue5");
      SimpleString squeue6 = new SimpleString("queue6");

      po.addBinding(condition1, squeue1, null, false, false);
      Map<SimpleString, List<Binding>> mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.addBinding(condition1, squeue2, null, false, false);
      mappings = po.getMappings();
      assertEquals(1, mappings.size());

      po.addBinding(condition1, squeue3, null, false, false);
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

      po.addBinding(condition2, squeue4, null, false, false);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      po.addBinding(condition2, squeue5, null, false, false);
      mappings = po.getMappings();
      assertEquals(2, mappings.size());

      final SimpleString condition3 = new SimpleString("topic.wibblexyz");

      po.addBinding(condition3, squeue6, null, false, false);
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

   class FakeFilter implements Filter
   {
      public SimpleString getFilterString()
      {
         return new SimpleString("aardvark");
      }

      public boolean match(ServerMessage message)
      {
         return true;
      }

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
         if(this.dests != null)
         {
            List<SimpleString> dests = (List<SimpleString>) EasyMock.getCurrentArguments()[2];
            dests.addAll(this.dests);
         }
         return null;
      }
   }
}
