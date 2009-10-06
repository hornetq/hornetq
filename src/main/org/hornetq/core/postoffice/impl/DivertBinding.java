/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.hornetq.core.postoffice.impl;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SimpleString;

/**
 * A LocalQueueBinding
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 28 Jan 2009 12:42:23
 *
 *
 */
public class DivertBinding implements Binding
{
   private final SimpleString address;
   
   private final Divert divert;
   
   private final Filter filter;
   
   private final SimpleString uniqueName;
   
   private final SimpleString routingName;
   
   private final boolean exclusive;
   
   private final long id;
   
   public DivertBinding(long id, final SimpleString address, final Divert divert)
   {
      this.id = id;

      this.address = address;
      
      this.divert = divert;
      
      this.filter = divert.getFilter();
      
      this.uniqueName = divert.getUniqueName();
      
      this.routingName = divert.getRoutingName();
      
      this.exclusive = divert.isExclusive();
   }
   
   public long getID()
   {
      return id;
   }
   
   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return divert;
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }
   
   public SimpleString getClusterName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

   public boolean isHighAcceptPriority(final ServerMessage message)
   {
      return true;
   }
   
   public void willRoute(final ServerMessage message)
   {      
   }

   public int getDistance()
   {
      return 0;
   }
   
   public BindingType getType()
   {
      return BindingType.DIVERT;
   }

}

