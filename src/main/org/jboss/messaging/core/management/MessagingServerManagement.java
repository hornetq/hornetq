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
package org.jboss.messaging.core.management;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.util.SimpleString;

/**
 * This interface describes the core management interface exposed by the server
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public interface MessagingServerManagement
{
   /**
    * is the server started
    * @return true if the server is running
    */
   boolean isStarted();

   /**
    * creates a queue with the specified address
    * @param address the address
    * @param name the name of the queue
    * @throws Exception if a problem occurred
    */
   void createQueue(SimpleString address, SimpleString name) throws Exception;

   /**
    * destroy a particular queue
    * @param name the name of the queue
    * @throws Exception if a problem occurred
    */
   void destroyQueue(SimpleString name) throws Exception;

   /**
    * add an address to the post office
    * @param address the address to add
    * @return true if the address was added
    * @throws Exception if a problem occurred
    */
   boolean addDestination(SimpleString address) throws Exception;

   /**
    * remove an address from the post office
    * @param address the address to remove
    * @return true if the address was removed
    * @throws Exception if a problem occurred
    */
   boolean removeDestination(SimpleString address) throws Exception;

   /**
    * returns all the queues for a specific address
    * @param address the address
    * @return the queues
    * @throws Exception if a problem occurred
    */
   List<Queue> getQueuesForAddress(SimpleString address) throws Exception;

   /**
    * remove all the messages for a specific address
    * @param address the address
    * @throws Exception if a problem occurred
    */
   void removeAllMessagesForAddress(SimpleString address) throws Exception;

   /**
    * remove all the messages for a specific binding
    * @param name the name of the binding
    * @throws Exception if a problem occurred
    */
   //void removeAllMessagesForBinding(SimpleString name) throws Exception;

   /**
    * List all messages in a queue that match the filter provided
    * @param queue the name of the queue
    * @param filter the filter
    * @return the messages
    * @throws Exception if a problem occurred
    */
   //List<Message> listMessages(SimpleString queue, Filter filter) throws Exception;

   /**
    * remove the messages for a specific binding that match the specified filter
    * @param binding the name of the binding
    * @param filter the filter
    * @throws Exception if a problem occurred
    */
  /// void removeMessageForBinding(String binding, Filter filter) throws Exception;

   /**
    * remove the messages for a specific address that match the specified filter
    * @param address the address
    * @param filter the filter
    * @throws Exception if a problem occurred
    */
  // void removeMessageForAddress(String address, Filter filter) throws Exception;

   /**
    * count the number of messages in a queue
    * @param queue the name of the queue
    * @return the number of messages in a queue
    * @throws Exception if a problem occurred
    */
   int getMessageCountForQueue(SimpleString queue) throws Exception;

   /**
    * register a message counter with a specific queue
    * @param queue the name of the queue
    * @throws Exception if a problem occurred
    */
   //void registerMessageCounter(SimpleString queue) throws Exception;

   /**
    * unregister a message counter from a specific queue
    * @param queue the name of the queue
    * @throws Exception if a problem occurred
    */
   //void unregisterMessageCounter(SimpleString queue) throws Exception;

   /**
    * start collection statistics on a message counter. The message counter must have been registered first.
    * @param queue the name of the queue
    * @param duration how long to take a sample for in seconds. 0 means indefinitely.
    * @throws Exception if a problem occurred
    */
   //void startMessageCounter(SimpleString queue, long duration) throws Exception;

   /**
    * stop a message counter on a specific queue. The message counter must be started to call this.
    * @param queue the name of the queue
    * @return the message counter stopped
    * @throws Exception if a problem occurred
    */
   //MessageCounter stopMessageCounter(SimpleString queue) throws Exception;

   /**
    * get a message counter for a specific queue
    * @param queue the name of the queue
    * @return the message counter
    */
   //MessageCounter getMessageCounter(SimpleString queue);
//
   /**
    * get all message counters
    * @return the message counters
    */
   //Collection<MessageCounter> getMessageCounters();

   /**
    * reset a message counter for a specific queue
    * @param queue the name of the queue
    */
   //void resetMessageCounter(SimpleString queue);

   /**
    * reset all message counters registered
    */
   //void resetMessageCounters();

   /**
    * reset the history for a message counter for a queue
    * @param queue the name of the queue
    */
   //void resetMessageCounterHistory(SimpleString queue);

   /**
    * reset all message counter histories
    */
   //void resetMessageCounterHistories();

   /**
    * stop all message counters
    * @return all message counters
    * @throws Exception if a problem occurred
    */
   //List<MessageCounter> stopAllMessageCounters() throws Exception;

   /**
    * unregister all message counters
    * @throws Exception if a problem occurred
    */
   //void unregisterAllMessageCounters() throws Exception;

   /**
    * get the number of consumers for a specific queue
    * @param queue the name of the queue
    * @return the count
    * @throws Exception if a problem occurred
    */
   //public int getConsumerCountForQueue(SimpleString queue) throws Exception;

   /**
    * return all the active connections
    * @return all connections
    */
   //List<ServerConnection> getActiveConnections();

   /**
    * move a set of messages from one queue to another
    * @param toQueue the source queue
    * @param fromQueue the destination queue
    * @param filter the filter to use
    * @throws Exception if a problem occurred
    */
   //void moveMessages(String toQueue, String fromQueue, String filter) throws Exception;

   /**
    * expire a set of messages for a specific queue
    * @param queue the name of the queue
    * @param filter the filter to use
    * @throws Exception if a problem occurred
    */
   //void expireMessages(SimpleString queue,String filter) throws Exception;

   /**
    * change the message priority for a set of messages
    * @param queue the name of the queue
    * @param filter the filter to use
    * @param priority the priority to change to
    * @throws Exception if a problem occurred
    */
   //void changeMessagePriority(String queue, String filter, int priority) throws Exception;

   /**
    * list all available addresses
    * @return the addresses
    */
   //Set<SimpleString> listAvailableAddresses();

   Configuration getConfiguration();

}
