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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

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
   
   String getExpiryQueue();
   
   String getDLQ();
   
   int getMessagesAdded();

   boolean isClustered();

   boolean isTemporary();

   boolean isDurable();

   long getSizeBytes();

   int getMaxSizeBytes();

   int getMessageCount();

   long getScheduledCount();

   int getConsumerCount();

   int getDeliveringCount();

   // Operations ----------------------------------------------------
   
   @Operation(desc = "List all messages in the queue", impact = INFO)
   TabularData listAllMessages() throws Exception;

   @Operation(desc = "List all messages in the queue which matches the filter", impact = INFO)
   TabularData listMessages(
         @Parameter(name = "filter", desc = "A JMS Message filter") String filter)
         throws Exception;

   @Operation(desc = "Remove all the messages from the queue", impact = ACTION)
   void removeAllMessages() throws Exception;

   @Operation(desc = "Remove the message corresponding to the given messageID", impact = ACTION)
   boolean removeMessage(
         @Parameter(name = "messageID", desc = "A message ID") String messageID)
         throws Exception;
   
   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = ACTION)
   int expireMessages(
         @Parameter(name = "filter", desc = "A message filter") String filter)
         throws Exception;

   @Operation(desc = "Expire the message corresponding to the given messageID", impact = ACTION)
   boolean expireMessage(
         @Parameter(name = "messageID", desc = "A message ID") String messageID)
         throws Exception;
   
   @Operation(desc = "Send the message corresponding to the given messageID to the queue's Dead Letter Queue", impact = ACTION)
   boolean sendMessageToDLQ(
         @Parameter(name = "messageID", desc = "A message ID") String messageID)
         throws Exception;
   
   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = ACTION)
   boolean changeMessagePriority(
         @Parameter(name = "messageID", desc = "A message ID") String messageID,
         @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority)
         throws Exception;

   CompositeData listMessageCounter();

   String listMessageCounterAsHTML();

   TabularData listMessageCounterHistory() throws Exception;

   String listMessageCounterHistoryAsHTML();


}
