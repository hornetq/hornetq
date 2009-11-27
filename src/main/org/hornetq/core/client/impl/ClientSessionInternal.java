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

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.utils.SimpleString;

/**
 * A ClientSessionInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionInternal extends ClientSession
{
   String getName();

   void acknowledge(long consumerID, long messageID) throws HornetQException;

   boolean isCacheLargeMessageClient();

   int getMinLargeMessageSize();

   void expire(long consumerID, long messageID) throws HornetQException;

   void addConsumer(ClientConsumerInternal consumer);

   void addProducer(ClientProducerInternal producer);

   void removeConsumer(ClientConsumerInternal consumer) throws HornetQException;

   void removeProducer(ClientProducerInternal producer);

   void handleReceiveMessage(long consumerID, SessionReceiveMessage message) throws Exception;

   void handleReceiveLargeMessage(long consumerID, SessionReceiveLargeMessage message) throws Exception;

   void handleReceiveContinuation(long consumerID, SessionReceiveContinuationMessage continuation) throws Exception;

   void handleFailover(RemotingConnection backupConnection);
   
   RemotingConnection getConnection();

   void cleanUp() throws Exception;

   void returnBlocking();
   
   void setForceNotSameRM(boolean force);
   
   FailoverManager getConnectionManager();
   
   void workDone();
   
   void forceDelivery(long consumerID, long sequence) throws HornetQException;
   
   void sendProducerCreditsMessage(int credits, SimpleString destination);
   
   ClientProducerCredits getCredits(SimpleString address);
   
   void handleReceiveProducerCredits(SimpleString address, int credits, int offset);
}
