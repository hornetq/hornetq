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

import java.util.Map;

import javax.management.MBeanOperationInfo;

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

   @Operation(desc = "List all messages in the queue which matches the filter", impact = MBeanOperationInfo.INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A JMS Message filter") String filter) throws Exception;

   @Operation(desc = "List all messages in the queue which matches the filter and return them using JSON", impact = MBeanOperationInfo.INFO)
   String listMessagesAsJSON(@Parameter(name = "filter", desc = "A JMS Message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = MBeanOperationInfo.INFO)
   int countMessages(@Parameter(name = "filter", desc = "A JMS message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = MBeanOperationInfo.ACTION)
   int removeMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = MBeanOperationInfo.ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Expire the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Send the message corresponding to the given messageID to the queue's Dead Letter Queue", impact = MBeanOperationInfo.ACTION)
   boolean sendMessageToDeadLetterAddress(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   @Operation(desc = "Send the messages corresponding to the given filter to this queue's Dead Letter Address", impact = MBeanOperationInfo.ACTION)
   int sendMessagesToDeadLetterAddress(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filterStr) throws Exception;

   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                                 @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   @Operation(desc = "Change the priority of the messages corresponding to the given filter", impact = MBeanOperationInfo.ACTION)
   int changeMessagesPriority(@Parameter(name = "filter", desc = "A message filter") String filter,
                              @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = MBeanOperationInfo.ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName) throws Exception;

   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = MBeanOperationInfo.ACTION)
   int moveMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName) throws Exception;

   @Operation(desc = "List the message counters", impact = MBeanOperationInfo.INFO)
   String listMessageCounter() throws Exception;

   @Operation(desc = "Reset the message counters", impact = MBeanOperationInfo.INFO)
   void resetMessageCounter() throws Exception;

   @Operation(desc = "List the message counters as HTML", impact = MBeanOperationInfo.INFO)
   String listMessageCounterAsHTML() throws Exception;

   @Operation(desc = "List the message counters history", impact = MBeanOperationInfo.INFO)
   String listMessageCounterHistory() throws Exception;

   @Operation(desc = "List the message counters history as HTML", impact = MBeanOperationInfo.INFO)
   String listMessageCounterHistoryAsHTML() throws Exception;

   @Operation(desc = "Pauses the queue.", impact = MBeanOperationInfo.INFO)
   void pause() throws Exception;

   @Operation(desc = "Returns true if the queue is paused.", impact = MBeanOperationInfo.INFO)
   boolean isPaused() throws Exception;

   @Operation(desc = "Resumes the queue.", impact = MBeanOperationInfo.INFO)
   void resume() throws Exception;

}
