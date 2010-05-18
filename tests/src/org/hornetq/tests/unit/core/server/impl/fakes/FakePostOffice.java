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

package org.hornetq.tests.unit.core.server.impl.fakes;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

public class FakePostOffice implements PostOffice
{

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#addBinding(org.hornetq.core.postoffice.Binding)
    */
   public void addBinding(final Binding binding) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getBinding(org.hornetq.utils.SimpleString)
    */
   public Binding getBinding(final SimpleString uniqueName)
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getBindingsForAddress(org.hornetq.utils.SimpleString)
    */
   public Bindings getBindingsForAddress(final SimpleString address) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getDuplicateIDCache(org.hornetq.utils.SimpleString)
    */
   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getMatchingBindings(org.hornetq.utils.SimpleString)
    */
   public Bindings getMatchingBindings(final SimpleString address)
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getNotificationLock()
    */
   public Object getNotificationLock()
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getPagingManager()
    */
   public PagingManager getPagingManager()
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#redistribute(org.hornetq.core.server.ServerMessage, org.hornetq.core.server.Queue, org.hornetq.core.server.RoutingContext)
    */
   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final RoutingContext context) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#removeBinding(org.hornetq.utils.SimpleString)
    */
   public Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#sendQueueInfoToQueue(org.hornetq.utils.SimpleString, org.hornetq.utils.SimpleString)
    */
   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#route(org.hornetq.core.server.ServerMessage)
    */
   public void route(final ServerMessage message) throws Exception
   {
      // TODO Auto-generated method stub

   }

   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final Transaction tx) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   public MessageReference reroute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {
      // TODO Auto-generated method stub

   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // TODO Auto-generated method stub

   }

   public void route(ServerMessage message, boolean direct) throws Exception
   {
      // TODO Auto-generated method stub
      
   }

   public void route(ServerMessage message, RoutingContext context, boolean direct) throws Exception
   {
      // TODO Auto-generated method stub
      
   }

   public void route(ServerMessage message, Transaction tx, boolean direct) throws Exception
   {
      // TODO Auto-generated method stub
      
   }

}