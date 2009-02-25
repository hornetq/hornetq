/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client;

import java.io.File;

import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.utils.SimpleString;

/*
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface ClientSession extends XAResource
{
   /**
    * Queues created by this method are <em>not</em> temporary
    */
   void createQueue(SimpleString address,
                    SimpleString queueName,
                    boolean durable) throws MessagingException;
   /**
    * Queues created by this method are <em>not</em> temporary
    */
   void createQueue(String address,
                    String queueName,
                    boolean durable) throws MessagingException;

   void createQueue(SimpleString address,
                    SimpleString queueName,
                    boolean durable,
                    boolean temporary) throws MessagingException;
   void createQueue(String address,
                    String queueName,
                    boolean durable,
                    boolean temporary) throws MessagingException;

   void createQueue(SimpleString address,
                    SimpleString queueName,
                    SimpleString filterString,
                    boolean durable,
                    boolean temporary) throws MessagingException;
   void createQueue(String address,
                    String queueName,
                    String filterString,
                    boolean durable,
                    boolean temporary) throws MessagingException;

   void deleteQueue(SimpleString queueName) throws MessagingException;
   void deleteQueue(String queueName) throws MessagingException;

   void addDestination(SimpleString address, boolean durable, boolean temporary) throws MessagingException;
   void addDestination(String address, boolean durable, boolean temporary) throws MessagingException;

   void removeDestination(SimpleString address, boolean durable) throws MessagingException;
   void removeDestination(String address, boolean durable) throws MessagingException;

   ClientConsumer createConsumer(SimpleString queueName) throws MessagingException;
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws MessagingException;
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws MessagingException;
   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filterString,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws MessagingException;

   ClientConsumer createConsumer(String queueName) throws MessagingException;
   ClientConsumer createConsumer(String queueName, String filterString) throws MessagingException;
   ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws MessagingException;
   ClientConsumer createConsumer(String queueName,
                                 String filterString,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws MessagingException;

   ClientConsumer createFileConsumer(File directory, SimpleString queueName) throws MessagingException;
   ClientConsumer createFileConsumer(File directory, SimpleString queueName, SimpleString filterString) throws MessagingException;
   ClientConsumer createFileConsumer(File directory,
                                     SimpleString queueName,
                                     SimpleString filterString,
                                     boolean browseOnly) throws MessagingException;
   ClientConsumer createFileConsumer(File directory,
                                     SimpleString queueName,
                                     SimpleString filterString,
                                     int windowSize,
                                     int maxRate,
                                     boolean browseOnly) throws MessagingException;

   ClientConsumer createFileConsumer(File directory, String queueName) throws MessagingException;
   ClientConsumer createFileConsumer(File directory, String queueName, String filterString) throws MessagingException;
   ClientConsumer createFileConsumer(File directory,
                                     String queueName,
                                     String filterString,
                                     boolean browseOnly) throws MessagingException;
   ClientConsumer createFileConsumer(File directory,
                                     String queueName,
                                     String filterString,
                                     int windowSize,
                                     int maxRate,
                                     boolean browseOnly) throws MessagingException;

   /**
    * Create a producer with no default address.
    * Address must be specified every time a message is sent
    * 
    * @see ClientProducer#send(SimpleString, org.jboss.messaging.core.message.Message)
    */
   ClientProducer createProducer() throws MessagingException;
   ClientProducer createProducer(SimpleString address) throws MessagingException;
   ClientProducer createProducer(SimpleString address, int rate) throws MessagingException;
   ClientProducer createProducer(SimpleString address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws MessagingException;

   ClientProducer createProducer(String address) throws MessagingException;
   ClientProducer createProducer(String address, int rate) throws MessagingException;
   ClientProducer createProducer(String address,
                                 int maxRate,
                                 boolean blockOnNonPersistentSend,
                                 boolean blockOnPersistentSend) throws MessagingException;

   SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws MessagingException;

   SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws MessagingException;

   XAResource getXAResource();

   void commit() throws MessagingException;

   void rollback() throws MessagingException;

   /**
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * @throws MessagingException
    */
   void rollback(boolean considerLastMessageAsDelivered) throws MessagingException;

   void close() throws MessagingException;

   boolean isClosed();

   boolean isAutoCommitSends();

   boolean isAutoCommitAcks();

   boolean isBlockOnAcknowledge();

   boolean isXA();

   ClientMessage createClientMessage(final byte type,
                                     final boolean durable,
                                     final long expiration,
                                     final long timestamp,
                                     final byte priority);

   ClientMessage createClientMessage(final byte type, final boolean durable);

   ClientMessage createClientMessage(final boolean durable);

   ClientFileMessage createFileMessage(final boolean durable);

   void start() throws MessagingException;

   void stop() throws MessagingException;

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);

   int getVersion();
   
   void setSendAcknowledgementHandler(SendAcknowledgementHandler handler);
}
