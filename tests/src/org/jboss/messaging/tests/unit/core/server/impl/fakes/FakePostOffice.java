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


package org.jboss.messaging.tests.unit.core.server.impl.fakes;

import java.util.List;

import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.utils.SimpleString;

public class FakePostOffice implements PostOffice
{

   public Object getNotificationLock()
   {
      return null;
   }

   public Bindings getMatchingBindings(SimpleString address)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#activate()
    */
   public List<Queue> activate()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#addBinding(org.jboss.messaging.core.postoffice.Binding)
    */
   public void addBinding(final Binding binding) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#getBinding(org.jboss.messaging.utils.SimpleString)
    */
   public Binding getBinding(final SimpleString uniqueName)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#getBindingsForAddress(org.jboss.messaging.utils.SimpleString)
    */
   public Bindings getBindingsForAddress(final SimpleString address) throws Exception
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#getDuplicateIDCache(org.jboss.messaging.utils.SimpleString)
    */
   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#getPagingManager()
    */
   public PagingManager getPagingManager()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#redistribute(org.jboss.messaging.core.server.ServerMessage, org.jboss.messaging.utils.SimpleString, org.jboss.messaging.core.transaction.Transaction)
    */
   public boolean redistribute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#removeBinding(org.jboss.messaging.utils.SimpleString)
    */
   public Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#route(org.jboss.messaging.core.server.ServerMessage)
    */
   public void route(final ServerMessage message) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#route(org.jboss.messaging.core.server.ServerMessage, org.jboss.messaging.core.transaction.Transaction)
    */
   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.postoffice.PostOffice#sendQueueInfoToQueue(org.jboss.messaging.utils.SimpleString, org.jboss.messaging.utils.SimpleString)
    */
   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.server.MessagingComponent#isStarted()
    */
   public boolean isStarted()
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.server.MessagingComponent#start()
    */
   public void start() throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.server.MessagingComponent#stop()
    */
   public void stop() throws Exception
   {

   }

}