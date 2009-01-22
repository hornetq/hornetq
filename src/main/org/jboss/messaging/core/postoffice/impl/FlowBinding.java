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

import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.impl.FlowBindingFilter;
import org.jboss.messaging.util.SimpleString;

/**
 * A FlowBinding
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 18:55:22
 *
 *
 */
public class FlowBinding extends BindingImpl
{
   private final FlowBindingFilter filter;

   public FlowBinding(final SimpleString address,
                      final SimpleString uniqueName,
                      final SimpleString routingName,
                      final Bindable bindable,
                      final FlowBindingFilter filter)
   {
      super(address, uniqueName, routingName, bindable, false, false);

      this.filter = filter;
   }

   public boolean accept(final ServerMessage message) throws Exception
   {
      if (filter.match(message))
      {
         return bindable.accept(message);
      }
      else
      {
         return false;
      }
   }
   
   public FlowBindingFilter getFilter()
   {
      return filter;
   }
}
