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

import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;

/**
 * This interface describes the management interface exposed by the server
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public interface MessagingServerManagement
{
   boolean isStarted();

   void createQueue(String address,String name) throws Exception;

   void destroyQueue(String name) throws Exception;
   
   boolean addAddress(String address) throws Exception;

   boolean removeAddress(String address) throws Exception;
   
   List<Queue> getQueuesForAddress(String address) throws Exception;

   ClientConnectionFactory createClientConnectionFactory(boolean strictTck,int prefetchSize, int producerWindowSize, int producerMaxRate);

   void removeAllMessagesForAddress(String address) throws Exception;

   void removeAllMessagesForBinding(String name) throws Exception;

   List<Message> listMessages(String queueName, Filter filter) throws Exception;

   void removeMessageForBinding(String binding, Filter filter) throws Exception;

   void removeMessageForAddress(String binding, Filter filter) throws Exception;

   int getMessageCountForQueue(String queue) throws Exception;

   void registerMessageCounter(String queueName) throws Exception;

   void unregisterMessageCounter(String queueName) throws Exception;

   void startMessageCounter(String queueName, long duration) throws Exception;

   MessageCounter stopMessageCounter(String queueName) throws Exception;

   MessageCounter getMessageCounter(String queueName);

   Collection<MessageCounter> getMessageCounters();

   void resetMessageCounter(String queue);

   void resetMessageCounters();

   void resetMessageCounterHistory(String queue);

   void resetMessageCounterHistories();

   List<MessageCounter> stopAllMessageCounters() throws Exception;

   void unregisterAllMessageCounters() throws Exception;

   public int getConsumerCountForQueue(String queue) throws Exception;

   List<ServerConnection> getActiveConnections();

   void moveMessages(String toQueue, String fromQueue, String filter) throws Exception;

   void expireMessages(String queue,String filter) throws Exception;

   void changeMessagePriority(String queue, String filter, int priority) throws Exception;

   void changeMessageHeader(String queue, String filter, String header, Object value) throws Exception;

   Set<String> listAvailableAddresses();

}
