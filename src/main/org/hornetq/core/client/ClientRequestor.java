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

import static org.hornetq.utils.SimpleString.toSimpleString;

import java.util.UUID;

import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.utils.SimpleString;


/**
 * a ClientRequestor.
 * 
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClientRequestor
{
   private final ClientSession queueSession;

   private final ClientProducer requestProducer;

   private final ClientConsumer replyConsumer;

   private final SimpleString replyQueue;

   public ClientRequestor(final ClientSession session, final SimpleString requestAddress) throws Exception
   {
      queueSession = session;

      requestProducer = queueSession.createProducer(requestAddress);
      replyQueue = new SimpleString(requestAddress + "." + UUID.randomUUID().toString());
      queueSession.createTemporaryQueue(replyQueue, replyQueue);
      replyConsumer = queueSession.createConsumer(replyQueue);
   }

   public ClientRequestor(final ClientSession session, final String requestAddress) throws Exception
   {
      this(session, toSimpleString(requestAddress));
   }

   public ClientMessage request(final ClientMessage request) throws Exception
   {
      return request(request, 0);
   }

   public ClientMessage request(final ClientMessage request, final long timeout) throws Exception
   {
      request.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, replyQueue);
      requestProducer.send(request);
      return replyConsumer.receive(timeout);
   }

   public void close() throws Exception
   {
      replyConsumer.close();
      queueSession.deleteQueue(replyQueue);
      queueSession.close();
   }

}
