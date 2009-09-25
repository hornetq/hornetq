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

import org.hornetq.core.exception.HornetQException;
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
   void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException;

   /**
    * Queues created by this method are <em>not</em> temporary
    */
   void createQueue(String address, String queueName) throws HornetQException;
   
   void createQueue(String address, String queueName, boolean durable) throws HornetQException;

   void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws HornetQException;

   void createQueue(String address, String queueName, String filterString, boolean durable) throws HornetQException;

   void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException;

   void createTemporaryQueue(String address, String queueName) throws HornetQException;

   void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException;

   void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException;

   void deleteQueue(SimpleString queueName) throws HornetQException;

   void deleteQueue(String queueName) throws HornetQException;

   ClientConsumer createConsumer(SimpleString queueName) throws HornetQException;

   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws HornetQException;

   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws HornetQException;
   
   ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws HornetQException;

   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filterString,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws HornetQException;

   ClientConsumer createConsumer(String queueName) throws HornetQException;

   ClientConsumer createConsumer(String queueName, String filterString) throws HornetQException;

   ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws HornetQException;
   
   ClientConsumer createConsumer(String queueName, boolean browseOnly) throws HornetQException;

   ClientConsumer createConsumer(String queueName, String filterString, int windowSize, int maxRate, boolean browseOnly) throws HornetQException;

   /**
    * Create a producer with no default address.
    * Address must be specified every time a message is sent
    * 
    * @see ClientProducer#send(SimpleString, org.hornetq.core.message.Message)
    */
   ClientProducer createProducer() throws HornetQException;

   ClientProducer createProducer(SimpleString address) throws HornetQException;

   ClientProducer createProducer(SimpleString address, int rate) throws HornetQException;

   ClientProducer createProducer(SimpleString address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws HornetQException;

   ClientProducer createProducer(String address) throws HornetQException;

   ClientProducer createProducer(String address, int rate) throws HornetQException;

   ClientProducer createProducer(String address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws HornetQException;

   SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws HornetQException;

   SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws HornetQException;

   XAResource getXAResource();

   void commit() throws HornetQException;

   void rollback() throws HornetQException;

   /**
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * @throws HornetQException
    */
   void rollback(boolean considerLastMessageAsDelivered) throws HornetQException;

   void close() throws HornetQException;

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

   void start() throws HornetQException;

   void stop() throws HornetQException;

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);

   int getVersion();

   void setSendAcknowledgementHandler(SendAcknowledgementHandler handler);
}
