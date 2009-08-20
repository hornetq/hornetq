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

package org.hornetq.jms.server.management;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

import java.util.Map;

import org.hornetq.core.management.Operation;
import org.hornetq.core.management.Parameter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface JMSQueueControl extends DestinationControl
{
   // Attributes ----------------------------------------------------

   String getName();

   String getExpiryAddress();

   void setExpiryAddress(@Parameter(name = "expiryAddress", desc = "Expiry address of the queue") String expiryAddress) throws Exception;

   String getDeadLetterAddress();

   void setDeadLetterAddress(@Parameter(name = "deadLetterAddress", desc = "Dead-letter address of the queue") String deadLetterAddress) throws Exception;

   int getMessagesAdded();

   boolean isTemporary();

   boolean isDurable();

   int getMessageCount();

   long getScheduledCount();

   int getConsumerCount();

   int getDeliveringCount();

   // Operations ----------------------------------------------------

   @Operation(desc = "List all messages in the queue which matches the filter", impact = INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A JMS Message filter") String filter) throws Exception;

   @Operation(desc = "List all messages in the queue which matches the filter and return them using JSON", impact = INFO)
   String listMessagesAsJSON(@Parameter(name = "filter", desc = "A JMS Message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = INFO)
   int countMessages(@Parameter(name = "filter", desc = "A JMS message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = ACTION)
   int removeMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Expire the message corresponding to the given messageID", impact = ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Send the message corresponding to the given messageID to the queue's Dead Letter Queue", impact = ACTION)
   boolean sendMessageToDeadLetterAddress(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                                 @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName) throws Exception;

   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = ACTION)
   int moveMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName) throws Exception;

   @Operation(desc = "List the message counters", impact = INFO)
   String listMessageCounter() throws Exception;

   @Operation(desc = "List the message counters as HTML", impact = INFO)
   String listMessageCounterAsHTML() throws Exception;

   @Operation(desc = "List the message counters history", impact = INFO)
   String listMessageCounterHistory() throws Exception;

   @Operation(desc = "List the message counters history as HTML", impact = INFO)
   String listMessageCounterHistoryAsHTML() throws Exception;

}
