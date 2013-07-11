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

package org.hornetq.api.core.client;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;

/**
 * A ClientProducer is used to send messages to a specific address. Messages are then routed on the
 * server to any queues that are bound to the address. A ClientProducer can either be created with a
 * specific address in mind or with none. With the latter the address must be provided using the
 * appropriate send() method. <br>
 * <p>
 * The sending semantics can change depending on what blocking semantics are set via
 * {@link ServerLocator#setBlockOnDurableSend(boolean)} and
 * {@link org.hornetq.api.core.client.ServerLocator#setBlockOnNonDurableSend(boolean)} . If set to
 * true then for each message type, durable and non durable respectively, any exceptions such as the
 * address not existing or security exceptions will be thrown at the time of send. Alternatively if
 * set to false then exceptions will only be logged on the server. <br>
 * <p>
 * The send rate can also be controlled via {@link ServerLocator#setProducerMaxRate(int)} and the
 * {@link org.hornetq.api.core.client.ServerLocator#setProducerWindowSize(int)}. <br>
 * <br>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public interface ClientProducer extends AutoCloseable
{
   /**
    * Returns the address where messages will be sent.
    *
    * <br><br>The address can be {@code null} if the ClientProducer
    *
    * was creating without specifying an address, that is by using {@link ClientSession#createProducer()}.
    *
    * @return the address where messages will be sent
    */
   SimpleString getAddress();

   /**
    * Sends a message to an address. specified in {@link ClientSession#createProducer(String)} or
    * similar methods. <br>
    * <br>
    * This will block until confirmation that the message has reached the server has been received
    * if {@link ServerLocator#setBlockOnDurableSend(boolean)} or
    * {@link ServerLocator#setBlockOnNonDurableSend(boolean)} are set to <code>true</code> for the
    * specified message type.
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(Message message) throws HornetQException;

   /**
    * Sends a message to the specified address instead of the ClientProducer's address. <br>
    * <br>
    * This message will be sent asynchronously.
    * <p>
    * The handler will only get called if {@link ServerLocator#setConfirmationWindowSize(int) -1}.
    * @param message the message to send
    * @param handler handler to call after receiving a SEND acknowledgement from the server
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(Message message, SendAcknowledgementHandler handler) throws HornetQException;

   /**
    * Sends a message to the specified address instead of the ClientProducer's address. <br>
    * <br>
    * This will block until confirmation that the message has reached the server has been received
    * if {@link ServerLocator#setBlockOnDurableSend(boolean)} or
    * {@link ServerLocator#setBlockOnNonDurableSend(boolean)} are set to true for the specified
    * message type.
    * @param address the address where the message will be sent
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(SimpleString address, Message message) throws HornetQException;

   /**
    * Sends a message to the specified address instead of the ClientProducer's address. <br>
    * <br>
    * This message will be sent asynchronously.
    * <p>
    * The handler will only get called if {@link ServerLocator#setConfirmationWindowSize(int) -1}.
    * @param address the address where the message will be sent
    * @param message the message to send
    * @param handler handler to call after receiving a SEND acknowledgement from the server
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(SimpleString address, Message message, SendAcknowledgementHandler handler) throws HornetQException;

   /**
    * Sends a message to the specified address instead of the ClientProducer's address. <br>
    * <br>
    * This will block until confirmation that the message has reached the server has been received
    * if {@link ServerLocator#setBlockOnDurableSend(boolean)} or
    * {@link ServerLocator#setBlockOnNonDurableSend(boolean)} are set to true for the specified
    * message type.
    * @param address the address where the message will be sent
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(String address, Message message) throws HornetQException;

   /**
    * Closes the ClientProducer. If already closed nothing is done.
    *
    * @throws HornetQException if an exception occurs while closing the producer
    */
   void close() throws HornetQException;

   /**
    * Returns whether the producer is closed or not.
    *
    * @return <code>true</code> if the producer is closed, <code>false</code> else
    */
   boolean isClosed();

   /**
    * Returns whether the producer will block when sending <em>durable</em> messages.
    *
    * @return <code>true</code> if the producer blocks when sending durable, <code>false</code> else
    */
   boolean isBlockOnDurableSend();

   /**
    * Returns whether the producer will block when sending <em>non-durable</em> messages.
    *
    * @return <code>true</code> if the producer blocks when sending non-durable, <code>false</code> else
    */
   boolean isBlockOnNonDurableSend();

   /**
    * Returns the maximum rate at which a ClientProducer can send messages per second.
    *
    * @return the producers maximum rate
    */
   int getMaxRate();
}
