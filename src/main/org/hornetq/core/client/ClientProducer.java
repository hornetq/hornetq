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

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.Message;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public interface ClientProducer
{
   /**
    * Return the address where messages will be sent to.
    * The address can be <code>null</code> if the ClientProducer
    * was creating without specifying an address with {@link ClientSession#createProducer()}. 
    * 
    * @return the address where messages will be sent
    */
   SimpleString getAddress();

   /**
    * Send a message.
    * 
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(Message message) throws HornetQException;

   /**
    * Send a message to the specified address instead of the ClientProducer's address.
    * 
    * @param address the address where the message will be sent
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(SimpleString address, Message message) throws HornetQException;

   /**
    * Send a message to the specified address instead of the ClientProducer's address.
    * 
    * @param address the address where the message will be sent
    * @param message the message to send
    * @throws HornetQException if an exception occurs while sending the message
    */
   void send(String address, Message message) throws HornetQException;

   /**
    * Close the ClientProducer.
    * 
    * @throws HornetQException if an exception occurs while closing the producer
    */
   void close() throws HornetQException;

   /**
    * Return whether the producer is closed or not.
    * 
    * @return <code>true</code> if the producer is closed, <code>false</code> else
    */
   boolean isClosed();

   /**
    * Return whether the producer will block when sending <em>persistent</em> messages.
    * 
    * @return <code>true</code> if the producer blocks when sending persistent, <code>false</code> else
    */
   boolean isBlockOnPersistentSend();

   /**
    * Return whether the producer will block when sending <em>non-persistent</em> messages.
    * 
    * @return <code>true</code> if the producer blocks when sending non-persistent, <code>false</code> else
    */
   boolean isBlockOnNonPersistentSend();

   /**
    * Return the producer maximum rate.
    * 
    * @return the producer maximum rate
    */
   int getMaxRate();
}
