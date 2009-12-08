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

import java.util.List;

import javax.transaction.xa.XAResource;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public interface ClientSession extends XAResource
{
   public interface BindingQuery
   {
      boolean isExists();

      public List<SimpleString> getQueueNames();
   }

   public interface QueueQuery
   {
      boolean isExists();

      boolean isDurable();

      int getConsumerCount();

      int getMessageCount();

      SimpleString getFilterString();

      SimpleString getAddress();
   }

   // Lifecycle operations ------------------------------------------

   /**
    * Start the session.
    * The session must be started before ClientConsumers created by the session can consume messages from the queue.
    * 
    * @throws HornetQException if an exception occurs while starting the session
    */
   void start() throws HornetQException;

   /**
    * Stop the session.
    * ClientConsumers created by the session can not consume messages when the session is stopped.
    * 
    * @throws HornetQException if an exception occurs while stopping the session
    */
   void stop() throws HornetQException;

   /**
    * Close the session.
    * 
    * @throws HornetQException if an exception occurs while closing the session
    */
   void close() throws HornetQException;

   /**
    * Indicate if the session is closed or not.
    * 
    * @return <code>true</code> if the session is closed, <code>false</code> else
    */
   boolean isClosed();

   /**
    * Add a FailureListener to the session which is notified if a failure occurs on the session.
    * 
    * @param listener the listener to add
    */
   void addFailureListener(SessionFailureListener listener);

   /**
    * Remove a FailureListener to the session.
    * 
    * @param listener the listener to remove
    * @return <code>true</code> if the listener was removed, <code>false</code> else
    */
   boolean removeFailureListener(SessionFailureListener listener);

   /**
    * Returns the server's incrementingVersion.
    *  
    * @return the server's <code>incrementingVersion</code>
    */
   int getVersion();

   // Queue Operations ----------------------------------------------

   /**
    * Create a queue. The created queue is <em>not</em> temporary.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable whether the queue is durable or not
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException;

   /**
    * Create a queue. The created queue is <em>not</em> temporary.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable whether the queue is durable or not
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName, boolean durable) throws HornetQException;

   /**
    * Create a queue. The created queue is <em>not</em> temporary and <em>not</em> durable.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName) throws HornetQException;

   /**
    * Create a queue. The created queue is <em>not</em> temporary.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable whether the queue is durable or not
    * @param filter only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, SimpleString queueName, SimpleString filter, boolean durable) throws HornetQException;

   /**
    * Create a queue. The created queue is <em>not</em> temporary.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable whether the queue is durable or not
    * @param filter only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName, String filter, boolean durable) throws HornetQException;

   /**
    * Create a <em>temporary</em> queue.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException;

   /**
    * Create a <em>temporary</em> queue.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, String queueName) throws HornetQException;

   /**
    * Create a <em>temporary</em> queue with a filter.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException;

   /**
    * Create a <em>temporary</em> queue with a filter.
    * 
    * @param address the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException;

   /**
    * Delete the queue.
    * 
    * @param queueName the name of the queue to delete
    * @throws HornetQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(SimpleString queueName) throws HornetQException;

   /**
    * Delete the queue.
    * 
    * @param queueName the name of the queue to delete
    * @throws HornetQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(String queueName) throws HornetQException;

   // Consumer Operations -------------------------------------------

   /**
    * Create a ClientConsumer to consume message from the queue with the given name.
    * 
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName) throws HornetQException;

   /**
    * Create a ClientConsumer to consume messages from the queue with the given name.
    * 
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName) throws HornetQException;

   /**
    * Create a ClientConsumer to consume messages matching the filter from the queue with the given name.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filter) throws HornetQException;

   /**
    * Create a ClientConsumer to consume messages matching the filter from the queue with the given name.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, boolean browseOnly) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages matching the filter from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter, boolean browseOnly) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages matching the filter from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filter, boolean browseOnly) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages matching the filter from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @param windowSize the consumer window size
    * @param maxRate the maximum rate to consume messages
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filter,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws HornetQException;

   /**
    * Create a ClientConsumer to consume or browse messages matching the filter from the queue with the given name.
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages from the queue
    * but they will not be consumed (the messages will remain in the queue).
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume the messages from the queue and
    * the messages will effectively be removed from the queue.
    * 
    * @param queueName name of the queue to consume messages from
    * @param filter only messages which match this filter will be consumed
    * @param windowSize the consumer window size
    * @param maxRate the maximum rate to consume messages
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter, int windowSize, int maxRate, boolean browseOnly) throws HornetQException;

   // Producer Operations -------------------------------------------

   /**
    * Create a producer with no default address.
    * Address must be specified every time a message is sent
    * 
    * @return a ClientProducer
    *
    * @see ClientProducer#send(SimpleString, org.hornetq.core.message.Message)
    */
   ClientProducer createProducer() throws HornetQException;

   /**
    * Create a produce which sends messages to the given address
    * 
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address) throws HornetQException;

   /**
    * Create a produce which sends messages to the given address
    * 
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(String address) throws HornetQException;

   /**
    * Create a produce which sends messages to the given address
    * 
    * @param address the address to send messages to
    * @param rate the producer rate
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address, int rate) throws HornetQException;

   // Message operations --------------------------------------------

   /**
    * Create a ClientMessage.
    * 
    * @param durable whether the created message is durable or not
    * @return a ClientMessage
    */
   ClientMessage createClientMessage(boolean durable);

   /**
    * Create a ClientMessage.
    * 
    * @param type type of the message
    * @param durable whether the created message is durable or not
    * @return a ClientMessage
    */
   ClientMessage createClientMessage(byte type, boolean durable);

   /**
    * Create a ClientMessage with the given HornetQBuffer as its body.
    * 
    * @param durable whether the created message is durable or not
    * @param expiration the message expiration
    * @param timestamp the message timestamp
    * @param priority the message priority (between 0 and 9 inclusive)
    * @return a ClientMessage
    */
   ClientMessage createClientMessage(byte type, boolean durable, long expiration, long timestamp, byte priority);

   // Query operations ----------------------------------------------

   QueueQuery queueQuery(SimpleString queueName) throws HornetQException;

   BindingQuery bindingQuery(SimpleString address) throws HornetQException;

   // Transaction operations ----------------------------------------

   /**
    * Returns the XAResource associated to the session.
    * 
    * @return the XAResource associated to the session
    */
   XAResource getXAResource();

   /**
    * Return <code>true</code> if the session supports XA, <code>false</code> else.
    * 
    * @return <code>true</code> if the session supports XA, <code>false</code> else.
    */
   boolean isXA();

   /**
    * Commit the current transaction.
    * 
    * @throws HornetQException if an exception occurs while committing the transaction
    */
   void commit() throws HornetQException;

   /**
    * Rollback the current transaction.
    * 
    * @throws HornetQException if an exception occurs while rolling back the transaction
    */
   void rollback() throws HornetQException;

   /**
    * Rollback the current transaction.
    * 
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * 
    * @throws HornetQException if an exception occurs while rolling back the transaction
    */
   void rollback(boolean considerLastMessageAsDelivered) throws HornetQException;

   /**
    * Return <code>true</code> if the current transaction has been flagged to rollback, <code>false</code> else.
    * 
    * @return <code>true</code> if the current transaction has been flagged to rollback, <code>false</code> else.
    */
   boolean isRollbackOnly();

   /**
    * Return whether the session will <em>automatically</em> commit its transaction every time a message is sent
    * by a ClientProducer created by this session, <code>false</code> else
    * 
    * @return <code>true</code> if the session <em>automatically</em> commit its transaction every time a message is sent, <code>false</code> else
    */
   boolean isAutoCommitSends();

   /**
    * Return whether the session will <em>automatically</em> commit its transaction every time a message is acknowledged
    * by a ClientConsumer created by this session, <code>false</code> else
    * 
    * @return <code>true</code> if the session <em>automatically</em> commit its transaction every time a message is acknowledged, <code>false</code> else
    */
   boolean isAutoCommitAcks();

   /**
    * Return whether the ClientConsumer created by the session will <em>block</em> when they acknowledge a message.
    * 
    * @return <code>true</code> if the session's ClientConsumer block when they acknowledge a message, <code>false</code> else
    */
   boolean isBlockOnAcknowledge();

   void setSendAcknowledgementHandler(SendAcknowledgementHandler handler);

}
