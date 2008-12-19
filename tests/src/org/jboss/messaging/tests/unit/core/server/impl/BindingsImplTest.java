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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.postoffice.impl.BindingsImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.Distributor;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * A BindingsImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Dec 2008 11:55:13
 *
 *
 */
public class BindingsImplTest extends UnitTestCase
{
   public void testGetBindings()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, false);
      Binding binding2 = new BindingImpl(address2, queue2, false);
      Binding binding3 = new BindingImpl(address3, queue3, false);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      List<Binding> theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(3, theBindings.size());
      assertTrue(theBindings.contains(binding1));
      assertTrue(theBindings.contains(binding2));
      assertTrue(theBindings.contains(binding3));           
   }
   
   public void testAddRemoveBindings()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, false);
      Binding binding2 = new BindingImpl(address2, queue2, false);
      Binding binding3 = new BindingImpl(address3, queue3, false);
      
      bindings.addBinding(binding1);

      List<Binding> theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(1, theBindings.size());
      assertTrue(theBindings.contains(binding1));
      
      bindings.addBinding(binding2);
      
      theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(2, theBindings.size());
      assertTrue(theBindings.contains(binding1));
      assertTrue(theBindings.contains(binding2));
      
      bindings.addBinding(binding3);

      theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(3, theBindings.size());
      assertTrue(theBindings.contains(binding1));
      assertTrue(theBindings.contains(binding2));
      assertTrue(theBindings.contains(binding3));
      
      bindings.removeBinding(binding1);
      
      theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(2, theBindings.size());
      assertTrue(theBindings.contains(binding2));
      assertTrue(theBindings.contains(binding3));
      
      bindings.removeBinding(binding2);
      
      theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(1, theBindings.size());      
      assertTrue(theBindings.contains(binding3));
      
      bindings.removeBinding(binding3);
      
      theBindings = bindings.getBindings();
      
      assertNotNull(theBindings);
      assertEquals(0, theBindings.size());      
             
   }
   
   public void testRouteNoBindings()
   {
      Bindings bindings = new BindingsImpl();
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);
      
      assertNotNull(refs);
      
      assertEquals(0, refs.size());
      
      Queue queue1 = new MyQueue();
           
      SimpleString address1 = new SimpleString("ushgduysd");
           
      Binding binding1 = new BindingImpl(address1, queue1, false);
      
      bindings.addBinding(binding1);
      
      bindings.removeBinding(binding1);

      refs = bindings.route(msg);
      
      assertNotNull(refs);
      
      assertEquals(0, refs.size());
      
      Binding binding2 = new BindingImpl(address1, queue1, true);
      
      bindings.addBinding(binding2);
      
      bindings.removeBinding(binding2);

      refs = bindings.route(msg);
      
      assertNotNull(refs);
      
      assertEquals(0, refs.size());      
   }
   
   public void testRouteNoExclusive()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, false);
      Binding binding2 = new BindingImpl(address2, queue2, false);
      Binding binding3 = new BindingImpl(address3, queue3, false);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);
      
      assertNotNull(refs);
      assertEquals(3, refs.size());
      
      Set<Queue> queues = new HashSet<Queue>();
      for (MessageReference ref: refs)
      {
         queues.add(ref.getQueue());
      }
      
      assertEquals(3, queues.size());
      assertTrue(queues.contains(queue1));
      assertTrue(queues.contains(queue2));
      assertTrue(queues.contains(queue3));           
   }
   
   public void testRouteAllExclusive()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, true);
      Binding binding2 = new BindingImpl(address2, queue2, true);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      bindings.removeBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      bindings.removeBinding(binding1);
      
      bindings.removeBinding(binding2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(0, refs.size()); 
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);            
   }
   
   
   public void testRouteOneExclusive()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, false);
      Binding binding2 = new BindingImpl(address2, queue2, false);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(2, refs.size());   
      
      Set<Queue> queues = new HashSet<Queue>();
      for (MessageReference ref: refs)
      {
         queues.add(ref.getQueue());
      }
      
      assertEquals(2, queues.size());
      assertTrue(queues.contains(queue1));
      assertTrue(queues.contains(queue2));  
      
      bindings.addBinding(binding3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);           
   }
   
   public void testRouteWithFilterNoExclusive()
   {
      Bindings bindings = new BindingsImpl();
      
      Filter filter1 = new MyFilter(false);
      
      Queue queue1 = new MyQueue(filter1);
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, false);
      Binding binding2 = new BindingImpl(address2, queue2, false);
      Binding binding3 = new BindingImpl(address3, queue3, false);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);
      
      assertNotNull(refs);
      assertEquals(2, refs.size());
      
      Set<Queue> queues = new HashSet<Queue>();
      for (MessageReference ref: refs)
      {
         queues.add(ref.getQueue());
      }
      
      assertEquals(2, queues.size());
      assertTrue(queues.contains(queue2));
      assertTrue(queues.contains(queue3));       
      
   }
   
   public void testRouteWithFilterExclusive()
   {
      Bindings bindings = new BindingsImpl();
      
      Filter filter1 = new MyFilter(false);
      
      Queue queue1 = new MyQueue(filter1);
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, true);
      Binding binding2 = new BindingImpl(address2, queue2, true);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding2);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding3);
      
      refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(0, refs.size());         
      
      
   }
   
   public void testRouteWithFilterExclusiveNoneMatch()
   {
      Bindings bindings = new BindingsImpl();
      
      Filter filter1 = new MyFilter(false);
      
      Queue queue1 = new MyQueue(filter1);
      Queue queue2 = new MyQueue(filter1);
      Queue queue3 = new MyQueue(filter1);
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, true);
      Binding binding2 = new BindingImpl(address2, queue2, true);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);               
      assertNotNull(refs);
      assertEquals(0, refs.size());   
     
   }
   
   public void testWeightedRoute()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, true);      
      Binding binding2 = new BindingImpl(address2, queue2, true);
      binding2.setWeight(2);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      bindings.removeBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      bindings.removeBinding(binding1);
      
      bindings.removeBinding(binding2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(0, refs.size()); 
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);            
   }
   
   public void testWeightedRoute2()
   {
      Bindings bindings = new BindingsImpl();
      
      Queue queue1 = new MyQueue();
      Queue queue2 = new MyQueue();
      Queue queue3 = new MyQueue();
      
      SimpleString address1 = new SimpleString("ushgduysd");
      SimpleString address2 = new SimpleString("asijisad");
      SimpleString address3 = new SimpleString("iqjdiqwjd");
      
      Binding binding1 = new BindingImpl(address1, queue1, true);      
      Binding binding2 = new BindingImpl(address2, queue2, true);
      binding2.setWeight(0);
      Binding binding3 = new BindingImpl(address3, queue3, true);
      
      bindings.addBinding(binding1);
      bindings.addBinding(binding2);
      bindings.addBinding(binding3);
      
      ServerMessage msg = new MyServerMessage();
      
      List<MessageReference> refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue3);
      
      bindings.removeBinding(binding3);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      bindings.removeBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(0, refs.size());         
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      bindings.removeBinding(binding1);
      
      bindings.removeBinding(binding2);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(0, refs.size()); 
      
      bindings.addBinding(binding1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);
      
      refs = bindings.route(msg);      
      assertNotNull(refs);
      assertEquals(1, refs.size());   
      assertTrue(refs.get(0).getQueue() == queue1);            
   }
   
   class MyFilter implements Filter
   {
      private boolean match;
      
      MyFilter(final boolean match)
      {
         this.match = match;
      }

      public SimpleString getFilterString()
      {
         return null;
      }

      public boolean match(ServerMessage message)
      {
         return match;
      }
      
   }
         
   class MyMessageReference implements MessageReference
   {
      private Queue queue;
      
      MyMessageReference(final Queue queue)
      {
         this.queue = queue;
      }
      
      public int getMemoryEstimate()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      
      public boolean cancel(StorageManager storageManager,
                            PostOffice postOffice,
                            HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public MessageReference copy(Queue queue)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void expire(StorageManager storageManager,
                         PostOffice postOffice,
                         HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void expire(Transaction tx,
                         StorageManager storageManager,
                         PostOffice postOffice,
                         HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public int getDeliveryCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public ServerMessage getMessage()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Queue getQueue()
      {         
         return queue;
      }

      public long getScheduledDeliveryTime()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void incrementDeliveryCount()
      {
         // TODO Auto-generated method stub
         
      }

      public void move(SimpleString toAddress, StorageManager persistenceManager, PostOffice postOffice) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void move(SimpleString toAddress, Transaction tx, StorageManager persistenceManager, boolean expiry) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void sendToDeadLetterAddress(StorageManager storageManager,
                                          PostOffice postOffice,
                                          HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void setDeliveryCount(int deliveryCount)
      {
         // TODO Auto-generated method stub
         
      }

      public void setScheduledDeliveryTime(long scheduledDeliveryTime)
      {
         // TODO Auto-generated method stub
         
      }
      
   }
   
   class MyServerMessage implements ServerMessage
   {

      public ServerMessage copy()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public MessageReference createReference(Queue queue)
      {
         return new MyMessageReference(queue);
      }

      public int decrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int decrementRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getMemoryEstimate()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int incrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void setMessageID(long id)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean containsProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void decode(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void decodeBody(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void decodeProperties(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void encode(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void encodeBody(MessagingBuffer buffer, long start, int size)
      {
         // TODO Auto-generated method stub
         
      }

      public void encodeBody(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void encodeProperties(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public MessagingBuffer getBody()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getBodySize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public SimpleString getDestination()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getExpiration()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getMessageID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public byte getPriority()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getPropertiesEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public Object getProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Set<SimpleString> getPropertyNames()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getTimestamp()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public byte getType()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public boolean isDurable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isExpired()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void putBooleanProperty(SimpleString key, boolean value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putByteProperty(SimpleString key, byte value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putBytesProperty(SimpleString key, byte[] value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putDoubleProperty(SimpleString key, double value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putFloatProperty(SimpleString key, float value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putIntProperty(SimpleString key, int value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putLongProperty(SimpleString key, long value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putShortProperty(SimpleString key, short value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putStringProperty(SimpleString key, SimpleString value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putTypedProperties(TypedProperties properties)
      {
         // TODO Auto-generated method stub
         
      }

      public Object removeProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void setBody(MessagingBuffer body)
      {
         // TODO Auto-generated method stub
         
      }

      public void setDestination(SimpleString destination)
      {
         // TODO Auto-generated method stub
         
      }

      public void setDurable(boolean durable)
      {
         // TODO Auto-generated method stub
         
      }

      public void setExpiration(long expiration)
      {
         // TODO Auto-generated method stub
         
      }

      public void setPriority(byte priority)
      {
         // TODO Auto-generated method stub
         
      }

      public void setTimestamp(long timestamp)
      {
         // TODO Auto-generated method stub
         
      }
      
   }
   
   
   class MyBinding implements Binding
   {
      private final SimpleString address;
      
      private final Queue queue;
      
      private int weight;
      
      private final boolean exclusive;
            
      public SimpleString getAddress()
      {
         return address;
      }

      public Queue getQueue()
      {
         return queue;
      }

      public int getWeight()
      {
         return weight;
      }

      public boolean isExclusive()
      {
         return exclusive;
      }

      public void setWeight(int weight)
      {
         this.weight = weight;
      }

      public MyBinding(final SimpleString address, final Queue queue, final int weight, final boolean exclusive)
      {
         this.address = address;
         this.queue = queue;
         this.weight = weight;
         this.exclusive = exclusive;
      }      
   }
   
   class MyQueue implements Queue
   {
      private Filter filter;
      
      MyQueue(Filter filter)
      {
         this.filter = filter;
      }
      
      MyQueue()
      {         
      }
      
      public void referenceHandled()
      {
         // TODO Auto-generated method stub
         
      }


      public boolean activate()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void activateNow(Executor executor)
      {
         // TODO Auto-generated method stub
         
      }

      public void addConsumer(Consumer consumer)
      {
         // TODO Auto-generated method stub
         
      }

      public HandleStatus addFirst(MessageReference ref)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void addListFirst(LinkedList<MessageReference> list)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean changeMessagePriority(long messageID,
                                           byte newPriority,
                                           StorageManager storageManager,
                                           PostOffice postOffice,
                                           HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean consumerFailedOver()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public int deleteAllReferences(StorageManager storageManager) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int deleteMatchingReferences(Filter filter, StorageManager storageManager) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public boolean deleteReference(long messageID, StorageManager storageManager) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void deliverAsync(Executor executor)
      {
         // TODO Auto-generated method stub
         
      }

      public void deliverNow()
      {
         // TODO Auto-generated method stub
         
      }

      public boolean expireMessage(long messageID,
                                   StorageManager storageManager,
                                   PostOffice postOffice,
                                   HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public int expireMessages(Filter filter,
                                StorageManager storageManager,
                                PostOffice postOffice,
                                HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void expireMessages(StorageManager storageManager,
                                 PostOffice postOffice,
                                 HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public int getConsumerCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getDeliveringCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public Distributor getDistributionPolicy()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Filter getFilter()
      {
         return filter;
      }

      public int getMessageCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getMessagesAdded()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public SimpleString getName()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getPersistenceID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public MessageReference getReference(long id)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getScheduledCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public List<MessageReference> getScheduledMessages()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean isBackup()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isClustered()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isDurable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isTemporary()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public List<MessageReference> list(Filter filter)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean moveMessage(long messageID,
                                 SimpleString toAddress,
                                 StorageManager storageManager,
                                 PostOffice postOffice) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public int moveMessages(Filter filter,
                              SimpleString toAddress,
                              StorageManager storageManager,
                              PostOffice postOffice) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void referenceAcknowledged(MessageReference ref) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void referenceCancelled()
      {
         // TODO Auto-generated method stub
         
      }

      public boolean removeConsumer(Consumer consumer) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public MessageReference removeFirst()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public MessageReference removeReferenceWithID(long id) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void rescheduleDelivery(long id, long scheduledDeliveryTime)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean sendMessageToDeadLetterAddress(long messageID,
                                                    StorageManager storageManager,
                                                    PostOffice postOffice,
                                                    HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void setBackup()
      {
         // TODO Auto-generated method stub
         
      }

      public void setDistributionPolicy(Distributor policy)
      {
         // TODO Auto-generated method stub
         
      }

      public void setPersistenceID(long id)
      {
         // TODO Auto-generated method stub
         
      }

      public HandleStatus add(MessageReference ref)
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      public int getMemoryEstimate()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      
   }
}
