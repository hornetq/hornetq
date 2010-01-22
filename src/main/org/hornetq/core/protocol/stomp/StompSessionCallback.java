/*
 * Copyright 2010 Red Hat, Inc.
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
package org.hornetq.core.protocol.stomp;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;

/**
 * A StompSessionCallback
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
class StompSessionCallback implements SessionCallback
{
   private final RemotingConnection connection;

   private final StompMarshaller marshaller;

   StompSessionCallback(final StompMarshaller marshaller, final RemotingConnection connection)
   {
      this.marshaller = marshaller;
      this.connection = connection;
   }

   public void sendProducerCreditsMessage(int credits, SimpleString address, int offset)
   {
   }

   public int sendMessage(ServerMessage serverMessage, long consumerID, int deliveryCount)
   {
      try
      {
         Map<String, Object> headers = new HashMap<String, Object>();
         headers.put(Stomp.Headers.Message.DESTINATION, StompUtils.toStompDestination(serverMessage.getAddress()
                                                                                                       .toString()));
         byte[] data = new byte[] {};
         if (serverMessage.getType() == Message.TEXT_TYPE)
         {
            SimpleString text = serverMessage.getBodyBuffer().readNullableSimpleString();
            if (text != null)
            {
               data = text.toString().getBytes();
            }
         }
         StompFrame msg = new StompFrame(Stomp.Responses.MESSAGE, headers, data);
         System.out.println("SENDING : " + msg);
         byte[] bytes = marshaller.marshal(msg);
         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bytes);
         connection.getTransportConnection().write(buffer, true);

         return bytes.length;

      }
      catch (Exception e)
      {
         e.printStackTrace();
         return 0;
      }

   }

   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      return 0;
   }

   public int sendLargeMessage(long consumerID, byte[] headerBuffer, long bodySize, int deliveryCount)
   {
      return 0;
   }

   public void closed()
   {
   }
}