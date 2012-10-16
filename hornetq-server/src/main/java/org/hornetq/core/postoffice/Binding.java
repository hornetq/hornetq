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

package org.hornetq.core.postoffice;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;

/**
 * 
 * A Binding
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Binding
{
   SimpleString getAddress();

   Bindable getBindable();

   BindingType getType();

   SimpleString getUniqueName();

   SimpleString getRoutingName();

   SimpleString getClusterName();

   Filter getFilter();

   boolean isHighAcceptPriority(ServerMessage message);

   boolean isExclusive();

   long getID();

   int getDistance();

   void route(ServerMessage message, RoutingContext context) throws Exception;
   
   void close() throws Exception;
}
