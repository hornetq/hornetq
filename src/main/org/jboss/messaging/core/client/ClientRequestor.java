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

import java.util.UUID;

import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * a ClientRequestor.
 * 
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClientRequestor
{
   private ClientSession queueSession;

   private ClientProducer requestProducer;

   private ClientConsumer replyConsumer;

   private SimpleString replyAddress;

   private SimpleString replyQueue;

   public ClientRequestor(ClientSession session, SimpleString requestAddress) throws Exception
   {
      queueSession = session;

      requestProducer = queueSession.createProducer(requestAddress);
      replyAddress = new SimpleString(UUID.randomUUID().toString());
      replyQueue = new SimpleString(UUID.randomUUID().toString());
      queueSession.addDestination(replyAddress, false, true);
      queueSession.createQueue(replyAddress, replyQueue, null, false, true, false);
      replyConsumer = queueSession.createConsumer(replyQueue);
   }

   public ClientMessage request(ClientMessage request) throws Exception
   {
      return request(request, 0);
   }

   public ClientMessage request(ClientMessage request, long timeout) throws Exception
   {
      request.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, replyAddress);
      requestProducer.send(request);
      return replyConsumer.receive(timeout);
   }

   public void close() throws Exception
   {
      try
      {
         replyConsumer.close();
      }
      catch (Exception ignored)
      {
      }
      try
      {
         queueSession.deleteQueue(replyQueue);
         queueSession.removeDestination(replyAddress, false);
      }
      catch (Exception ignored)
      {
      }
      queueSession.close();
   }

 }
