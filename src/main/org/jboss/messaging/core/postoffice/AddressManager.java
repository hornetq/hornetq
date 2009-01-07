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

import org.jboss.messaging.util.SimpleString;

import java.util.Map;
import java.util.Set;

/**
 * Used to maintain addresses and Bindings.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface AddressManager
{
   void addBinding(Binding binding);

   boolean addMapping(SimpleString address, Binding binding);

   Bindings getBindings(SimpleString address);

   void clear();

   Binding removeBinding(SimpleString queueName);

   boolean removeMapping(SimpleString address, SimpleString queueName);

   boolean addDestination(SimpleString address);

   boolean removeDestination(SimpleString address);

   boolean containsDestination(SimpleString address);

   Set<SimpleString> getDestinations();

   Binding getBinding(SimpleString queueName);

   Map<SimpleString, Binding> getBindings();
}
