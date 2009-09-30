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

import java.util.Collection;

import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

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

   boolean route(ServerMessage message, Transaction tx) throws Exception;

   void addBinding(Binding binding);

   void removeBinding(Binding binding);

   void setRouteWhenNoConsumers(boolean takePriorityIntoAccount);

   boolean redistribute(ServerMessage message, Queue originatingQueue, Transaction tx) throws Exception;
}
