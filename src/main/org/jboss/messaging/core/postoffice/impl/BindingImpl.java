/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.postoffice.impl;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * A BindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 18:52:15
 *
 *
 */
public class BindingImpl implements Binding
{
   private final SimpleString address;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   protected final Bindable bindable;

   private final boolean exclusive;

   private final boolean isQueue;

   public BindingImpl(final SimpleString address,
                      final SimpleString uniqueName,
                      final SimpleString routingName,
                      final Bindable bindable,                      
                      final boolean exclusive,
                      final boolean isQueue)
   {
      this.address = address;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.bindable = bindable;

      this.exclusive = exclusive;

      this.isQueue = isQueue;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return bindable;
   }

   public boolean isQueueBinding()
   {
      return isQueue;
   }

   public boolean accept(final ServerMessage message) throws Exception
   {
      return bindable.accept(message);
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

}
