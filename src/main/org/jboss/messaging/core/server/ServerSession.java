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
package org.jboss.messaging.core.server;

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;

/**
 * 
 * A ServerSession
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ServerSession
{
	String getID();
	
	void removeBrowser(String browserID) throws Exception;
	
	void removeConsumer(String consumerID) throws Exception;
	
	void removeProducer(String producerID) throws Exception;
	
	void close() throws Exception;
	
	void setStarted(boolean started) throws Exception;
	
	void handleDelivery(MessageReference reference, ServerConsumer consumer) throws Exception;
	
	void promptDelivery(Queue queue);
	
	void send(String address, Message msg) throws Exception;

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

   void addDestination(String address, boolean temporary) throws Exception;

   void removeDestination(String address, boolean temporary) throws Exception;

   void createQueue(String address, String queueName, String filterString, boolean durable, boolean temporary) throws Exception;

   void deleteQueue(String queueName) throws Exception;

   SessionCreateConsumerResponseMessage createConsumer(String queueName, String filterString, boolean noLocal,
   		                                              boolean autoDeleteQueue, int windowSize, int maxRate) throws Exception;
   
   SessionCreateProducerResponseMessage createProducer(String address, int windowSize, int maxRate) throws Exception;   

   SessionQueueQueryResponseMessage executeQueueQuery(SessionQueueQueryMessage request) throws Exception;

   SessionBindingQueryResponseMessage executeBindingQuery(SessionBindingQueryMessage request) throws Exception;

   SessionCreateBrowserResponseMessage createBrowser(String queueName, String selector) throws Exception;
}
