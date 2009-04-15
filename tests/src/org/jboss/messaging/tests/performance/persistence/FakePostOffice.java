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

package org.jboss.messaging.tests.performance.persistence;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.utils.ConcurrentHashSet;
import org.jboss.messaging.utils.SimpleString;

/**
 *
 * A FakePostOffice
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class FakePostOffice implements PostOffice
{
   public boolean redistribute(ServerMessage message, SimpleString routingName, Transaction tx) throws Exception
   {
     
      return false;
   }

   public void sendQueueInfoToQueue(SimpleString queueName, SimpleString address) throws Exception
   {
     
      
   }

   private ConcurrentHashMap<SimpleString, Binding> bindings = new ConcurrentHashMap<SimpleString, Binding>();

   private org.jboss.messaging.utils.ConcurrentHashSet<SimpleString> addresses = new ConcurrentHashSet<SimpleString>();

   private volatile boolean started;

   public void addBinding(Binding binding) throws Exception
   {
      bindings.put(binding.getAddress(), binding);
   }

   public void route(ServerMessage message, Transaction tx) throws Exception
   {
   }

   public void route(ServerMessage message) throws Exception
   {
   }

   public boolean addDestination(SimpleString address, boolean temporary) throws Exception
   {
      return addresses.addIfAbsent(address);
   }

   public boolean containsDestination(SimpleString address)
   {
      return addresses.contains(address);
   }

   public Binding getBinding(SimpleString queueName)
   {
      return bindings.get(queueName);
   }

   public Bindings getBindingsForAddress(SimpleString address) throws Exception
   {
      return null;
   }

   public List<Queue> getQueues()
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

   public boolean removeDestination(SimpleString address, boolean temporary) throws Exception
   {
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

   public List<Queue> activate()
   {
      return null;
   }

   public PagingManager getPagingManager()
   {
      return null;
   }

   public DuplicateIDCache getDuplicateIDCache(SimpleString address)
   {
      return null;
   }

   public void sendQueueInfoToQueue(SimpleString queueName) throws Exception
   {
   }

   
}
