/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server;

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.server.impl.ServerBrowserImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ServerSession
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ServerSession
{
	long getID();
	
	String getUsername();
	
	String getPassword();
	
	void removeBrowser(ServerBrowserImpl browser) throws Exception;
	
	void removeConsumer(ServerConsumer consumer) throws Exception;
	
	void removeProducer(ServerProducer producer) throws Exception;
	
	void close() throws Exception;
	
	void setStarted(boolean started) throws Exception;
	
	void handleDelivery(MessageReference reference, ServerConsumer consumer) throws Exception;
	
	void deliverDeferredDelivery(long messageID);
	
	void promptDelivery(Queue queue);
	
	void send(ServerMessage msg) throws Exception;

   void acknowledge(long deliveryID, boolean allUpTo) throws Exception;

   void rollback() throws Exception;

   void cancel(long deliveryID, boolean expired) throws Exception;

   void commit() throws Exception;

   SessionXAResponseMessage XACommit(boolean onePhase, Xid xid) throws Exception;

   SessionXAResponseMessage XAEnd(Xid xid, boolean failed) throws Exception;

   SessionXAResponseMessage XAForget(Xid xid);

   SessionXAResponseMessage XAJoin(Xid xid) throws Exception;

   SessionXAResponseMessage XAPrepare(Xid xid) throws Exception;

   SessionXAResponseMessage XAResume(Xid xid) throws Exception;

   SessionXAResponseMessage XARollback(Xid xid) throws Exception;

   SessionXAResponseMessage XAStart(Xid xid);

   SessionXAResponseMessage XASuspend() throws Exception;

   List<Xid> getInDoubtXids() throws Exception;

   int getXATimeout();

   boolean setXATimeout(int timeoutSeconds);

   void addDestination(SimpleString address, boolean durable, boolean temporary) throws Exception;

   void removeDestination(SimpleString address, boolean durable) throws Exception;

   void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString,
                    boolean durable, boolean temporary) throws Exception;

   void deleteQueue(SimpleString queueName) throws Exception;

   SessionCreateConsumerResponseMessage createConsumer(SimpleString queueName, SimpleString filterString,
   		                                              int windowSize, int maxRate) throws Exception;
   
   SessionCreateProducerResponseMessage createProducer(SimpleString address, int windowSize, int maxRate) throws Exception;   

   SessionQueueQueryResponseMessage executeQueueQuery(SimpleString queueName) throws Exception;

   SessionBindingQueryResponseMessage executeBindingQuery(SimpleString address) throws Exception;

   void createBrowser(SimpleString queueName, SimpleString filterString) throws Exception;
   
   void closeConsumer(long consumerID) throws Exception;
   
   void closeProducer(long producerID) throws Exception;
   
   void closeBrowser(long browserID) throws Exception;
   
   void receiveConsumerCredits(long consumerID, int credits) throws Exception;
   
   void sendProducerMessage(long producerID, ServerMessage message) throws Exception;
   
   boolean browserHasNextMessage(long browserID) throws Exception;
   
   ServerMessage browserNextMessage(long browserID) throws Exception;
   
   void browserReset(long browserID) throws Exception;
   
   void handleReplicateDelivery(long consumerID, long messageID) throws Exception;
   
   void handleDeferredDelivery();
   
   void transferConnection(RemotingConnection newConnection);
   
   int replayCommands(int lastReceivedCommandID);
}
