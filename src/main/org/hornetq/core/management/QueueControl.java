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

package org.hornetq.core.management;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

import java.util.Map;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface QueueControl
{
   // Attributes ----------------------------------------------------

   String getName();

   String getAddress();

   long getPersistenceID();

   boolean isTemporary();

   boolean isDurable();

   String getFilter();

   int getMessageCount();

   long getScheduledCount();

   int getConsumerCount();

   int getDeliveringCount();

   int getMessagesAdded();

   String getExpiryAddress();

   void setExpiryAddress(@Parameter(name = "expiryAddress", desc = "Expiry address of the queue") String expiryAddres) throws Exception;

   String getDeadLetterAddress();

   void setDeadLetterAddress(@Parameter(name = "deadLetterAddress", desc = "Dead-letter address of the queue") String deadLetterAddress) throws Exception;

   boolean isBackup();

   // Operations ----------------------------------------------------

   @Operation(desc = "List the messages scheduled for delivery", impact = INFO)
   Map<String, Object>[] listScheduledMessages() throws Exception;

   @Operation(desc = "List the messages scheduled for delivery and returns them using JSON", impact = INFO)
   String listScheduledMessagesAsJSON() throws Exception;

   @Operation(desc = "List all the messages in the queue matching the given filter", impact = INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "List all the messages in the queue matching the given filter and returns them using JSON", impact = INFO)
   String listMessagesAsJSON(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = INFO)
   int countMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = ACTION)
   int removeMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of expired messages)", impact = ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter") String filter) throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName) throws Exception;

   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = ACTION)
   int moveMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName) throws Exception;

   @Operation(desc = "Send the message corresponding to the given messageID to this queue's Dead Letter Address", impact = ACTION)
   boolean sendMessageToDeadLetterAddress(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   int sendMessagesToDeadLetterAddress(String filterStr) throws Exception;

   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID") long messageID,
                                 @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   @Operation(desc = "Change the priority of the messages corresponding to the given filter", impact = ACTION)
   int changeMessagesPriority(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                              @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   @Operation(desc = "List the message counters", impact = INFO)
   String listMessageCounter() throws Exception;

   @Operation(desc = "Reset the message counters", impact = INFO)
   void resetMessageCounter() throws Exception;

   @Operation(desc = "List the message counters as HTML", impact = INFO)
   String listMessageCounterAsHTML() throws Exception;

   @Operation(desc = "List the message counters history", impact = INFO)
   String listMessageCounterHistory() throws Exception;

   @Operation(desc = "List the message counters history HTML", impact = INFO)
   String listMessageCounterHistoryAsHTML() throws Exception;
}
