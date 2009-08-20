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

package org.hornetq.core.client;

import javax.transaction.xa.XAResource;

import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.utils.SimpleString;

/*
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface ClientSession extends XAResource
{
   /**
    * Queues created by this method are <em>not</em> temporary
    */
   void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws MessagingException;

   /**
    * Queues created by this method are <em>not</em> temporary
    */
   void createQueue(String address, String queueName) throws MessagingException;
   
   void createQueue(String address, String queueName, boolean durable) throws MessagingException;

   void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws MessagingException;

   void createQueue(String address, String queueName, String filterString, boolean durable) throws MessagingException;

   void createTemporaryQueue(SimpleString address, SimpleString queueName) throws MessagingException;

   void createTemporaryQueue(String address, String queueName) throws MessagingException;

   void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws MessagingException;

   void createTemporaryQueue(String address, String queueName, String filter) throws MessagingException;

   void deleteQueue(SimpleString queueName) throws MessagingException;

   void deleteQueue(String queueName) throws MessagingException;

   ClientConsumer createConsumer(SimpleString queueName) throws MessagingException;

   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws MessagingException;

   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws MessagingException;

   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filterString,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws MessagingException;

   ClientConsumer createConsumer(String queueName) throws MessagingException;

   ClientConsumer createConsumer(String queueName, String filterString) throws MessagingException;

   ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws MessagingException;

   ClientConsumer createConsumer(String queueName, String filterString, int windowSize, int maxRate, boolean browseOnly) throws MessagingException;

   /**
    * Create a producer with no default address.
    * Address must be specified every time a message is sent
    * 
    * @see ClientProducer#send(SimpleString, org.hornetq.core.message.Message)
    */
   ClientProducer createProducer() throws MessagingException;

   ClientProducer createProducer(SimpleString address) throws MessagingException;

   ClientProducer createProducer(SimpleString address, int rate) throws MessagingException;

   ClientProducer createProducer(SimpleString address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws MessagingException;

   ClientProducer createProducer(String address) throws MessagingException;

   ClientProducer createProducer(String address, int rate) throws MessagingException;

   ClientProducer createProducer(String address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws MessagingException;

   SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws MessagingException;

   SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws MessagingException;

   XAResource getXAResource();

   void commit() throws MessagingException;

   void rollback() throws MessagingException;

   /**
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * @throws MessagingException
    */
   void rollback(boolean considerLastMessageAsDelivered) throws MessagingException;

   void close() throws MessagingException;

   boolean isClosed();

   boolean isAutoCommitSends();

   boolean isAutoCommitAcks();

   boolean isBlockOnAcknowledge();

   boolean isXA();

   ClientMessage createClientMessage(final byte type,
                                     final boolean durable,
                                     final long expiration,
                                     final long timestamp,
                                     final byte priority);

   ClientMessage createClientMessage(final byte type, final boolean durable);

   ClientMessage createClientMessage(final boolean durable);

   void start() throws MessagingException;

   void stop() throws MessagingException;

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);

   int getVersion();

   void setSendAcknowledgementHandler(SendAcknowledgementHandler handler);
}
