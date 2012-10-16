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

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;

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

   public DivertBinding(final long id, final SimpleString address, final Divert divert)
   {
      this.id = id;

      this.address = address;

      this.divert = divert;

      filter = divert.getFilter();

      uniqueName = divert.getUniqueName();

      routingName = divert.getRoutingName();

      exclusive = divert.isExclusive();
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

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      divert.route(message, context);
   }

   public int getDistance()
   {
      return 0;
   }

   public BindingType getType()
   {
      return BindingType.DIVERT;
   }


   
   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "DivertBinding [id=" + id +
             ", address=" +
             address +
             ", divert=" +
             divert +
             ", filter=" +
             filter +
             ", uniqueName=" +
             uniqueName +
             ", routingName=" +
             routingName +
             ", exclusive=" +
             exclusive +
             "]";
   }

   public void close() throws Exception
   {    
   }

}
