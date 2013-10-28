/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.group;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;
import org.hornetq.core.server.management.NotificationListener;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface GroupingHandler extends NotificationListener, HornetQComponent
{
   SimpleString getName();

   Response propose(Proposal proposal) throws Exception;

   void proposed(Response response) throws Exception;

   void send(Response response, int distance) throws Exception;

   Response receive(Proposal proposal, int distance) throws Exception;

   void addGroupBinding(GroupBinding groupBinding);

   Response getProposal(SimpleString fullID);

   void awaitBindings() throws Exception;

   void remove(SimpleString groupid, SimpleString clusterName, int distance) throws Exception;
}
