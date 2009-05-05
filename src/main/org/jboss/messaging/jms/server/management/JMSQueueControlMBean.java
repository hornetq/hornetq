/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.management;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

import java.util.Map;

import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface JMSQueueControlMBean extends DestinationControlMBean
{
   // Attributes ----------------------------------------------------

   String getName();

   String getExpiryAddress();

   void setExpiryAddress(@Parameter(name = "expiryAddress", desc = "Expiry address of the queue")
   String expiryAddress) throws Exception;

   String getDeadLetterAddress();

   void setDeadLetterAddress(@Parameter(name = "deadLetterAddress", desc = "Dead-letter address of the queue")
   String deadLetterAddress) throws Exception;

   int getMessagesAdded();

   boolean isTemporary();

   boolean isDurable();

   int getMessageCount();

   long getScheduledCount();

   int getConsumerCount();

   int getDeliveringCount();

   // Operations ----------------------------------------------------

   @Operation(desc = "List all messages in the queue", impact = INFO)
   Map<String, Object>[] listAllMessages() throws Exception;

   @Operation(desc = "List all messages in the queue which matches the filter", impact = INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A JMS Message filter")
   String filter) throws Exception;

   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = INFO)
   int countMessages(@Parameter(name = "filter", desc = "A JMS message filter")
   String filter) throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID")
   String messageID) throws Exception;

   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = ACTION)
   int removeMatchingMessages(@Parameter(name = "filter", desc = "A message filter")
   String filter) throws Exception;

   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter")
   String filter) throws Exception;

   @Operation(desc = "Expire the message corresponding to the given messageID", impact = ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID")
   String messageID) throws Exception;

   @Operation(desc = "Send the message corresponding to the given messageID to the queue's Dead Letter Queue", impact = ACTION)
   boolean sendMessageToDLQ(@Parameter(name = "messageID", desc = "A message ID")
   String messageID) throws Exception;

   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID")
   String messageID, @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)")
   int newPriority) throws Exception;

   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID")
   String messageID, @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to")
   String otherQueueName) throws Exception;

   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = ACTION)
   int moveMatchingMessages(@Parameter(name = "filter", desc = "A message filter")
   String filter, @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to")
   String otherQueueName) throws Exception;

   @Operation(desc = "Move all the messages to another queue (and returns the number of moved messages)", impact = ACTION)
   int moveAllMessages(@Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to")
   String otherQueueName) throws Exception;

   Object[] listMessageCounter() throws Exception;

   String listMessageCounterAsHTML() throws Exception;

   Object[] listMessageCounterHistory() throws Exception;

   String listMessageCounterHistoryAsHTML() throws Exception;

}
