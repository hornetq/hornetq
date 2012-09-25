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

package org.hornetq.core.client.impl;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;

/**
 * 
 * A ClientConsumerInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientConsumerInternal extends ClientConsumer
{
   long getID();

   SimpleString getQueueName();

   SimpleString getFilterString();

   boolean isBrowseOnly();

   void handleMessage(ClientMessageInternal message) throws Exception;

   void handleLargeMessage(SessionReceiveLargeMessage largeMessageHeader) throws Exception;

   void handleLargeMessageContinuation(SessionReceiveContinuationMessage continuation) throws Exception;

   void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws HornetQException;

   void clear(boolean waitForOnMessage) throws HornetQException;

   /**
    * To be called by things like MDBs during shutdown of the server
    * @param interrupt
    * @throws HornetQException
    */
   void interruptHandlers() throws HornetQException;

   void clearAtFailover();

   int getClientWindowSize();

   int getBufferSize();

   void cleanUp() throws HornetQException;

   void acknowledge(ClientMessage message) throws HornetQException;

   void individualAcknowledge(ClientMessage message) throws HornetQException;

   void flushAcks() throws HornetQException;

   void stop() throws HornetQException;

   void stop(boolean waitForOnMessage) throws HornetQException;

   void start();
   
   SessionQueueQueryResponseMessage getQueueInfo();
   
   ClientSessionInternal getSession();
}
