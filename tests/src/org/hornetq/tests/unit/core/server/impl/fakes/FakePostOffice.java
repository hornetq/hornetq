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


package org.hornetq.tests.unit.core.server.impl.fakes;

import java.util.List;

import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.SimpleString;

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
    * @see org.hornetq.core.postoffice.PostOffice#activate()
    */
   public List<Queue> activate()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#addBinding(org.hornetq.core.postoffice.Binding)
    */
   public void addBinding(final Binding binding) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getBinding(org.hornetq.utils.SimpleString)
    */
   public Binding getBinding(final SimpleString uniqueName)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getBindingsForAddress(org.hornetq.utils.SimpleString)
    */
   public Bindings getBindingsForAddress(final SimpleString address) throws Exception
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getDuplicateIDCache(org.hornetq.utils.SimpleString)
    */
   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#getPagingManager()
    */
   public PagingManager getPagingManager()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#redistribute(org.hornetq.core.server.ServerMessage, org.hornetq.utils.SimpleString, org.hornetq.core.transaction.Transaction)
    */
   public boolean redistribute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#removeBinding(org.hornetq.utils.SimpleString)
    */
   public Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#route(org.hornetq.core.server.ServerMessage)
    */
   public void route(final ServerMessage message) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.transaction.Transaction)
    */
   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.postoffice.PostOffice#sendQueueInfoToQueue(org.hornetq.utils.SimpleString, org.hornetq.utils.SimpleString)
    */
   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessagingComponent#isStarted()
    */
   public boolean isStarted()
   {
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessagingComponent#start()
    */
   public void start() throws Exception
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessagingComponent#stop()
    */
   public void stop() throws Exception
   {

   }

}