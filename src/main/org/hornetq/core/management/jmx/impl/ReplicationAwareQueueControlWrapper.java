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

package org.hornetq.core.management.jmx.impl;

import java.util.Map;

import javax.management.MBeanInfo;

import org.hornetq.core.management.QueueControl;
import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.core.management.impl.QueueControlImpl;

/**
 * A ReplicationAwareQueueControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareQueueControlWrapper extends ReplicationAwareStandardMBeanWrapper implements QueueControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final QueueControlImpl localQueueControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareQueueControlWrapper(final QueueControlImpl localControl, 
                                              final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.CORE_QUEUE + localControl.getName(), QueueControl.class, replicationInvoker);

      this.localQueueControl = localControl;
   }

   // QueueControlMBean implementation ------------------------------

   public int getConsumerCount()
   {
      return localQueueControl.getConsumerCount();
   }

   public String getDeadLetterAddress()
   {
      return localQueueControl.getDeadLetterAddress();
   }

   public void setDeadLetterAddress(String deadLetterAddress) throws Exception
   {
      replicationAwareInvoke("setDeadLetterAddress", deadLetterAddress);
   }
   
   public int getDeliveringCount()
   {
      return localQueueControl.getDeliveringCount();
   }

   public String getExpiryAddress()
   {
      return localQueueControl.getExpiryAddress();
   }
   
   public void setExpiryAddress(String expiryAddres) throws Exception
   {
      replicationAwareInvoke("setExpiryAddress", expiryAddres);
   }

   public String getFilter()
   {
      return localQueueControl.getFilter();
   }

   public int getMessageCount()
   {
      return localQueueControl.getMessageCount();
   }

   public int getMessagesAdded()
   {
      return localQueueControl.getMessagesAdded();
   }

   public String getName()
   {
      return localQueueControl.getName();
   }

   public String getAddress()
   {
      return localQueueControl.getAddress();
   }
   
   public long getPersistenceID()
   {
      return localQueueControl.getPersistenceID();
   }

   public long getScheduledCount()
   {
      return localQueueControl.getScheduledCount();
   }

   public boolean isBackup()
   {
      return localQueueControl.isBackup();
   }

   public boolean isDurable()
   {
      return localQueueControl.isDurable();
   }

   public boolean isTemporary()
   {
      return localQueueControl.isTemporary();
   }

   public String listMessageCounter() throws Exception
   {
      return localQueueControl.listMessageCounter();
   }
   
   public void resetMessageCounter() throws Exception
   {
      localQueueControl.resetMessageCounter();
   }

   public String listMessageCounterAsHTML() throws Exception
   {
      return localQueueControl.listMessageCounterAsHTML();
   }

   public String listMessageCounterHistory() throws Exception
   {
      return localQueueControl.listMessageCounterHistory();
   }

   public String listMessageCounterHistoryAsHTML() throws Exception
   {
      return localQueueControl.listMessageCounterHistoryAsHTML();
   }

   public Map<String, Object>[] listMessages(final String filter) throws Exception
   {
      return localQueueControl.listMessages(filter);
   }
   
   public String listMessagesAsJSON(String filter) throws Exception
   {
      return localQueueControl.listMessagesAsJSON(filter);
   }
   
   public int countMessages(final String filter) throws Exception
   {
      return localQueueControl.countMessages(filter);
   }

   public Map<String, Object>[] listScheduledMessages() throws Exception
   {
      return localQueueControl.listScheduledMessages();
   }
   
   public String listScheduledMessagesAsJSON() throws Exception
   {
      return localQueueControl.listScheduledMessagesAsJSON();
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
   {
      return (Boolean)replicationAwareInvoke("changeMessagePriority", messageID, newPriority);
   }
   
   public int changeMessagesPriority(String filter, int newPriority) throws Exception
   {
      return (Integer)replicationAwareInvoke("changeMessagesPriority", filter, newPriority);
   }

   public boolean expireMessage(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("expireMessage", messageID);
   }

   public int expireMessages(final String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("expireMessages", filter);
   }

   public int moveMessages(final String filter, final String otherQueueName) throws Exception
   {
      return (Integer)replicationAwareInvoke("moveMessages", filter, otherQueueName);
   }

   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception
   {
      return (Boolean)replicationAwareInvoke("moveMessage", messageID, otherQueueName);
   }

   public int removeMessages(final String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("removeMessages", filter);
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("removeMessage", messageID);
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("sendMessageToDeadLetterAddress", messageID);
   }
   
   public int sendMessagesToDeadLetterAddress(String filterStr) throws Exception
   {
      return (Integer)replicationAwareInvoke("sendMessagesToDeadLetterAddress", filterStr);
   }

   // StandardMBean overrides ---------------------------------------

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(QueueControl.class),
                           info.getNotifications());
   }

   public boolean isPaused() throws Exception
   {
      return localQueueControl.isPaused();
   }

   public void pause() throws Exception
   {
      localQueueControl.pause();
   }

   public void resume() throws Exception
   {
      localQueueControl.resume();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
