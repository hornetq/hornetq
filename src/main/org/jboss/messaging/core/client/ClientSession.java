/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
