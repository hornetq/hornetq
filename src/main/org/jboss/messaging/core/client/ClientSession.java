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

package org.jboss.messaging.core.client;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.XAResource;

/**
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public interface ClientSession extends XAResource
{   
   void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable, boolean temporary)
                    throws MessagingException;
   
   void deleteQueue(SimpleString queueName) throws MessagingException;
   
   void addDestination(SimpleString address, boolean temporary) throws MessagingException;
   
   void removeDestination(SimpleString address, boolean temporary) throws MessagingException;
   
   SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws MessagingException;
   
   SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws MessagingException;
               
   ClientConsumer createConsumer(SimpleString queueName) throws MessagingException;
   
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean noLocal,
                                 boolean autoDeleteQueue, boolean direct) throws MessagingException;
   
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean noLocal,
                                 boolean autoDeleteQueue, boolean direct,
                                 int windowSize, int maxRate) throws MessagingException;
   
   ClientBrowser createBrowser(SimpleString queueName, SimpleString filterString) throws MessagingException;
   
   ClientBrowser createBrowser(SimpleString queueName) throws MessagingException;
   
   ClientProducer createProducer(SimpleString address) throws MessagingException;
   
   ClientProducer createProducer(SimpleString address, int windowSize, int maxRate,
                                 boolean blockOnNonPersistentSend, boolean blockOnPersistentSend) throws MessagingException;
   
   ClientProducer createRateLimitedProducer(SimpleString address, int rate) throws MessagingException;
   
   ClientProducer createProducerWithWindowSize(SimpleString address, int windowSize) throws MessagingException;
   
   XAResource getXAResource();

   void commit() throws MessagingException;

   void rollback() throws MessagingException;
      
   void acknowledge() throws MessagingException;
   
   void close() throws MessagingException;
   
   boolean isClosed();     
   
   boolean isAutoCommitSends();
   
   boolean isAutoCommitAcks();
   
   boolean isBlockOnAcknowledge();
   
   boolean isCacheProducers();
   
   int getLazyAckBatchSize();
   
   boolean isXA();

   void cleanUp();
}
