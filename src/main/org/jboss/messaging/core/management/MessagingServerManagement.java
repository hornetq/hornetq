/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 * This interface describes the core management interface exposed by the server
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface MessagingServerManagement
{
   int getConnectionCount();
   
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
    * @param address
    * @param name
    * @param filter
    * @param durable
    * @param temporary
    * @throws Exception
    */
   void createQueue(SimpleString address, SimpleString name, SimpleString filter,
         boolean durable, boolean temporary) throws Exception;

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

   boolean removeMessageFromQueue(long messageID, SimpleString queueName) throws Exception;

   /**
    * remove all the messages for a specific address
    * @param address the address
    * @throws Exception if a problem occurred
    */
   void removeAllMessagesForAddress(SimpleString address) throws Exception;

   void setSecurityForAddress(SimpleString address, Set<Role> roles) throws Exception;
   
   void removeSecurityForAddress(SimpleString address) throws Exception;
   
   Set<Role> getSecurityForAddress(SimpleString address) throws Exception;
   
   void setQueueAttributes(SimpleString queueName, QueueSettings settings) throws Exception;
   
   Configuration getConfiguration();

   /**
    * @param queueName
    * @return
    * @throws Exception 
    */
   Queue getQueue(SimpleString queueName) throws Exception;

   /**
    * @return
    */
   String getVersion();

   boolean expireMessage(long messageID, SimpleString queueName) throws Exception;


   int expireMessages(Filter filter,
         SimpleString queueName) throws Exception;

   /**
    * @param simpleAddress
    * @return
    */
   QueueSettings getQueueSettings(SimpleString simpleAddress);

   int sendMessagesToDLQ(Filter filter,
         SimpleString queueName) throws Exception;

   int changeMessagesPriority(Filter filter,
         byte newPriority, SimpleString queueName) throws Exception;
}
