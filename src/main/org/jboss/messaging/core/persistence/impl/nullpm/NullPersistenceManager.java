/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.persistence.impl.nullpm;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.server.Binding;
import org.jboss.messaging.core.server.Message;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.PersistenceManager;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;

import java.util.List;
import java.util.ArrayList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class NullPersistenceManager implements PersistenceManager
{
   public long generateMessageID()
   {
      return 0;
   }

   public void addMessage(Message message) throws Exception
   {

   }

   public void deleteReference(MessageReference reference) throws Exception
   {

   }

   public void commitTransaction(List<Message> messagesToAdd, List<MessageReference> referencesToRemove) throws Exception
   {

   }

   public void prepareTransaction(Xid xid, List<Message> messagesToAdd, List<MessageReference> referencesToRemove) throws Exception
   {

   }

   public void commitPreparedTransaction(Xid xid) throws Exception
   {

   }

   public void unprepareTransaction(Xid xid, List<Message> messagesToAdd, List<MessageReference> referencesToRemove) throws Exception
   {

   }

   public void updateDeliveryCount(Queue queue, MessageReference ref) throws Exception
   {

   }

   public void deleteAllReferences(Queue queue) throws Exception
   {

   }

   public List<Xid> getInDoubtXids() throws Exception
   {
      return new ArrayList<Xid>();
   }

   public boolean isInRecoveryMode() throws Exception
   {
      return false;
   }

   public void setInRecoveryMode(boolean recoveryMode)
   {

   }

   public List<Binding> loadBindings(QueueFactory queueFactory) throws Exception
   {
      return new ArrayList<Binding>();
   }

   public void addBinding(Binding binding) throws Exception
   {

   }

   public void deleteBinding(Binding binding) throws Exception
   {

   }

   public void start() throws Exception
   {

   }

   public void stop() throws Exception
   {
      
   }
}
