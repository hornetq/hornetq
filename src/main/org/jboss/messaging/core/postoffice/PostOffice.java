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

package org.jboss.messaging.core.postoffice;

import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.SendLock;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A PostOffice instance maintains a mapping of a String address to a Queue. Multiple Queue instances can be bound
 * with the same String address.
 * 
 * Given a message and an address a PostOffice instance will route that message to all the Queue instances that are
 * registered with that address.
 * 
 * Addresses can be any String instance.
 * 
 * A Queue instance can only be bound against a single address in the post office.
 * 
 * The PostOffice also maintains a set of "allowable addresses". These are the addresses that it is legal to
 * route to.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface PostOffice extends MessagingComponent
{
   boolean addDestination(SimpleString address, boolean durable) throws Exception;

   boolean removeDestination(SimpleString address, boolean durable) throws Exception;

   boolean containsDestination(SimpleString address);

   Binding addLinkBinding(SimpleString name,
                          SimpleString address,
                          Filter filter,
                          boolean durable,
                          boolean temporary,
                          boolean exclusive,
                          SimpleString linkAddress,
                          boolean duplicateDetection) throws Exception;

   Binding addQueueBinding(SimpleString name,
                           SimpleString address,
                           Filter filter,
                           boolean durable,
                           boolean temporary,
                           boolean exclusive) throws Exception;

   Binding removeBinding(SimpleString name) throws Exception;

   Bindings getBindingsForAddress(SimpleString address) throws Exception;

   Binding getBinding(SimpleString name);

   void route(ServerMessage message) throws Exception;
   
   void route(ServerMessage message, Transaction tx) throws Exception;

   Set<SimpleString> listAllDestinations();

   List<Queue> activate();

   PagingManager getPagingManager();

   SendLock getAddressLock(SimpleString address);

   DuplicateIDCache getDuplicateIDCache(SimpleString address);

   int numMappings();
}
