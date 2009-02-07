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

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.BindingType;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class FakeBinding implements Binding
{
   
   public SimpleString getClusterName()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public BindingType getType()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public int getDistance()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   public SimpleString getOriginatingNodeID()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public Filter getFilter()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public int getID()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   public void setID(int id)
   {
      // TODO Auto-generated method stub
      
   }

   public void willRoute(ServerMessage message)
   {
      // TODO Auto-generated method stub
      
   }

   public boolean filterMatches(ServerMessage message) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   public boolean isHighAcceptPriority(ServerMessage message)
   {
      // TODO Auto-generated method stub
      return false;
   }

   public SimpleString getRoutingName()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public SimpleString getUniqueName()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public boolean isExclusive()
   {
      // TODO Auto-generated method stub
      return false;
   }

   private SimpleString address;

   private Bindable bindable;

   public FakeBinding(final SimpleString address, final Bindable bindable)
   {
      this.address = address;
      this.bindable = bindable;
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
      return true;
   }
}
