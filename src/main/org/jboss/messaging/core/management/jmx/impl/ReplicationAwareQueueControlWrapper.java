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

package org.jboss.messaging.core.management.jmx.impl;

import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.impl.QueueControl;

/**
 * A ReplicationAwareQueueControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationAwareQueueControlWrapper extends ReplicationAwareStandardMBeanWrapper implements QueueControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final QueueControl localQueueControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareQueueControlWrapper(final ObjectName objectName, final QueueControl localControl) throws Exception
   {
      super(objectName, QueueControlMBean.class);

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

   public long getPersistenceID()
   {
      return localQueueControl.getPersistenceID();
   }

   public long getScheduledCount()
   {
      return localQueueControl.getScheduledCount();
   }

   public long getSizeBytes()
   {
      return localQueueControl.getSizeBytes();
   }

   public boolean isBackup()
   {
      return localQueueControl.isBackup();
   }

   public boolean isClustered()
   {
      return localQueueControl.isClustered();
   }

   public boolean isDurable()
   {
      return localQueueControl.isDurable();
   }

   public boolean isTemporary()
   {
      return localQueueControl.isTemporary();
   }

   public TabularData listAllMessages() throws Exception
   {
      return localQueueControl.listAllMessages();
   }

   public CompositeData listMessageCounter() throws Exception
   {
      return localQueueControl.listMessageCounter();
   }

   public String listMessageCounterAsHTML() throws Exception
   {
      return localQueueControl.listMessageCounterAsHTML();
   }

   public TabularData listMessageCounterHistory() throws Exception
   {
      return localQueueControl.listMessageCounterHistory();
   }

   public String listMessageCounterHistoryAsHTML() throws Exception
   {
      return localQueueControl.listMessageCounterHistoryAsHTML();
   }

   public TabularData listMessages(final String filter) throws Exception
   {
      return localQueueControl.listMessages(filter);
   }
   
   public int countMessages(final String filter) throws Exception
   {
      return localQueueControl.countMessages(filter);
   }

   public TabularData listScheduledMessages() throws Exception
   {
      return localQueueControl.listScheduledMessages();
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
   {
      return (Boolean)replicationAwareInvoke("changeMessagePriority", messageID, newPriority);
   }

   public boolean expireMessage(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("expireMessage", messageID);
   }

   public int expireMessages(final String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("expireMessages", filter);
   }

   public int moveAllMessages(final String otherQueueName) throws Exception
   {
      return (Integer)replicationAwareInvoke("moveAllMessages", otherQueueName);
   }

   public int moveMatchingMessages(final String filter, final String otherQueueName) throws Exception
   {
      return (Integer)replicationAwareInvoke("moveMatchingMessages", filter, otherQueueName);
   }

   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception
   {
      return (Boolean)replicationAwareInvoke("moveMessage", messageID, otherQueueName);
   }

   public int removeAllMessages() throws Exception
   {
      return (Integer)replicationAwareInvoke("removeAllMessages");
   }

   public int removeMatchingMessages(final String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("removeMatchingMessages", filter);
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("removeMessage", messageID);
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("sendMessageToDeadLetterAddress", messageID);
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
                           MBeanInfoHelper.getMBeanOperationsInfo(QueueControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
