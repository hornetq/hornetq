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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;

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

   void handleMessage(ClientMessageInternal message) throws Exception;

   void handleLargeMessage(SessionReceiveMessage largeMessageHeader) throws Exception;
   
   void handleLargeMessageContinuation(SessionReceiveContinuationMessage continuation) throws Exception;
   
   void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws MessagingException;

   void clear();

   int getClientWindowSize();

   int getBufferSize();

   void cleanUp() throws MessagingException;
   
   void acknowledge(ClientMessage message) throws MessagingException;
   
   void flushAcks() throws MessagingException;

   void stop() throws MessagingException;

   void start();
}
