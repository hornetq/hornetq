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

import java.util.List;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * An Exchange
 * 
 * It is the responsibility of the exchange to maintain bindings between names and queues, and handle
 * message routing to queues.
 * 
 * For a standard point to point queue, the Queue would be bound to a name that represents the name
 * of the queue, e.g. "MyQueue".
 * 
 * If the Queue represents a publish-subscribe subscription then it might be bound to the name
 * "MyTopic.subscription123".
 * 
 * Then we handling the message, a routing key is specified.
 * 
 * The routing key might be a simple name like "MyQueue" in which case the message would get sent to MyQueue
 * or it might be a wildcard expression e.g. "MyTopic.*" - in which case the message would get sent to any 
 * Queues bound to any names that start with "MyTopic."
 * 
 * In such a way we can provide the semantics of JMS Queues and Topics without having to create separate Queue
 * and Topic constructs which vastly simplifies the code.
 * 
 * It also acts as a placeholder for much more complex routing policies in the future.
 * 
 * Finally, it makes clustering simpler, since we only have to worry about clustering the Queue.
 * 
 * For now we only support one binding per queue.
 * 
 * If we are to support full blown AMQP style message exchanges then we would
 * need to support more than one binding per queue.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public interface Exchange extends ServerPlugin
{   
   /**
    * Creates a new MessageQueue and binds it to the exchange
    * If a binding with the queueName already exist an exception is thrown
    * @param queueName
    * @param condition
    * @param selector
    * @param noLocal
    * @param durable
    * @return The binding
    * @throws Exception
    */
   Binding bindQueue(String queueName, String condition, Filter filter, boolean noLocal, boolean durable,
                     MessageStore ms, PersistenceManager pm,
                     int fullSize, int pageSize, int downCacheSize) throws Exception;
   
   /**
    * Removes the queue and all its data and unbinds it from the exchange
    * @param queueName
    * @return
    * @throws Exception
    */
   Binding unbindQueue(String queueName) throws Throwable;
   
   /**
    * Reloads any message queues whose condition matches the wildcard.
    * @param wildcard
    * @param ms
    * @param pm
    * @param mm
    * @param fullSize
    * @param pageSize
    * @param downCacheSize
    * @throws Exception
    */
   void reloadQueues(String wildcard,
                     MessageStore ms, PersistenceManager pm,
                     int fullSize, int pageSize, int downCacheSize) throws Exception;
   
   /**
    * Unloads any queues for any bindings whose condition matches the wildcard
    * @param wildcard
    * @throws Exception
    */
   void unloadQueues(String wildcard) throws Exception;
   
   /**
    * List all bindings whose condition matches the wildcard
    * @param wildcard
    * @return
    * @throws Exception
    */
   List listBindingsForWildcard(String wildcard) throws Exception;
   
   
   /**
    * Returns binding that matches the name
    * @param queueName
    * @return
    * @throws Exception
    */
   Binding getBindingForName(String queueName) throws Exception;
   
   /**
    * Route a message
    * @param message
    * @param routingKey
    * @param tx
    * @return
    * @throws Exception
    */
   boolean route(MessageReference ref, String routingKey, Transaction tx) throws Exception;   
}
