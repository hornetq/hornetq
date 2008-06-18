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

package org.jboss.messaging.tests.performance.persistence.fakes;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeQueueFactory;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A FakePostOffice
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class FakePostOffice implements PostOffice
{
   private ConcurrentHashMap<SimpleString, Binding> bindings = new ConcurrentHashMap<SimpleString, Binding>();
   
   private QueueFactory queueFactory = new FakeQueueFactory();
   
   private ConcurrentHashSet<SimpleString> addresses = new ConcurrentHashSet<SimpleString>();
   
   private volatile boolean started;
   
   public Binding addBinding(SimpleString address, SimpleString queueName,
         Filter filter, boolean durable, boolean temporary) throws Exception
   {
      Queue queue = queueFactory.createQueue(-1, queueName, filter, durable, temporary); 
      Binding binding = new FakeBinding(address, queue);
      bindings.put(address, binding);
      return binding;
   }

   public boolean addDestination(SimpleString address, boolean temporary)
         throws Exception
   {
      return addresses.addIfAbsent(address);
   }

   public boolean containsDestination(SimpleString address)
   {
      return addresses.contains(address);
   }

   public Binding getBinding(SimpleString queueName) throws Exception
   {
      return bindings.get(queueName);
   }

   public List<Binding> getBindingsForAddress(SimpleString address)
         throws Exception
   {
      return null;
   }

   public FlowController getFlowController(SimpleString address)
   {
      return null;
   }

   public Map<SimpleString, List<Binding>> getMappings()
   {
      return null;
   }

   public Set<SimpleString> listAllDestinations()
   {
      return null;
   }

   public Binding removeBinding(SimpleString queueName) throws Exception
   {
      return null;
   }

   public boolean removeDestination(SimpleString address, boolean temporary)
         throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   public void start() throws Exception
   {
      started = true;
   }

   public void stop() throws Exception
   {
      started = false;
   }
   
   public boolean isStarted()
   {
      return started;
   }

   public List<org.jboss.messaging.core.server.MessageReference> route(
         ServerMessage message) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }
   
}
