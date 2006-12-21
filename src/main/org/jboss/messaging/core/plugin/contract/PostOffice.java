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
package org.jboss.messaging.core.plugin.contract;

import java.util.Collection;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * A PostOffice
 * 
 * A post office holds bindings of queues to conditions.
 * 
 * When routing a reference, the post office routes the reference to any binding whose
 * condition matches the condition specified in the call to route(...)
 * 
 * Currently we only support conditions where the condition is an exact text match, and
 * there is a single binding per queue.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public interface PostOffice extends MessagingComponent
{
   String getOfficeName();
   
   Binding bindQueue(Condition condition, Queue queue) throws Exception;

   Binding unbindQueue(String queueName) throws Throwable;

   /**
    * List the bindings that match the specified condition
    * @param condition
    * @return
    * @throws Exception
    */
   Collection listBindingsForCondition(Condition condition) throws Exception;
   
   /**
    * Get the binding for the specified queue name
    * @param queueName
    * @return
    * @throws Exception
    */
   Binding getBindingForQueueName(String queueName) throws Exception;

   /**
    * Route a reference.
    * @param ref
    * @param condition The message will be routed to a queue if specified condition matches the condition
    * of the binding
    * 
    * @param tx The transaction or null if not in the context of a transaction
    * @return true if ref was accepted by at least one queue
    * @throws Exception
    */
   boolean route(MessageReference ref, Condition condition, Transaction tx) throws Exception; 
   
   /**
    * 
    * @return true if it is a non clustered post office
    */
   boolean isLocal();
   
   Binding getBindingforChannelId(long channelId) throws Exception;
}
