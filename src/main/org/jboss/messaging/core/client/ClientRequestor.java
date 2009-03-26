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
package org.jboss.messaging.core.client;

import static org.jboss.messaging.utils.SimpleString.toSimpleString;
import org.jboss.messaging.utils.SimpleString;

import java.util.UUID;

import org.jboss.messaging.core.client.impl.ClientMessageImpl;

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
      replyQueue = new SimpleString(UUID.randomUUID().toString());
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
