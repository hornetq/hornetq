/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.hornetq.core.client.impl;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.spi.MessagingBuffer;

/**
 * A ClientSessionInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionInternal extends ClientSession
{
   String getName();

   void acknowledge(long consumerID, long messageID) throws MessagingException;

   boolean isCacheLargeMessageClient();

   int getMinLargeMessageSize();

   MessagingBuffer createBuffer(int size);

   void expire(long consumerID, long messageID) throws MessagingException;

   void addConsumer(ClientConsumerInternal consumer);

   void addProducer(ClientProducerInternal producer);

   void removeConsumer(ClientConsumerInternal consumer) throws MessagingException;

   void removeProducer(ClientProducerInternal producer);

   void handleReceiveMessage(long consumerID, SessionReceiveMessage message) throws Exception;

   void handleReceiveLargeMessage(long consumerID, SessionReceiveMessage message) throws Exception;

   void handleReceiveContinuation(long consumerID, SessionReceiveContinuationMessage continuation) throws Exception;

   boolean handleFailover(RemotingConnection backupConnection);

   RemotingConnection getConnection();

   void cleanUp() throws Exception;

   void returnBlocking();
   
   void setForceNotSameRM(boolean force);
   
   ConnectionManager getConnectionManager();
}
