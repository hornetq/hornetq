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

import java.util.Collection;

import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.utils.SimpleString;

/**
 * A Bindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 10 Dec 2008 19:10:52
 *
 *
 */
public interface Bindings
{
   Collection<Binding> getBindings();

   void route(ServerMessage message, Transaction tx) throws Exception;

   void addBinding(Binding binding);

   void removeBinding(Binding binding);

   void setRouteWhenNoConsumers(boolean takePriorityIntoAccount);

   boolean redistribute(ServerMessage message, SimpleString routingName, Transaction tx) throws Exception;
}
