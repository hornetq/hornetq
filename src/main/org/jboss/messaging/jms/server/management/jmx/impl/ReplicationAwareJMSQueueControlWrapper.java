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

package org.jboss.messaging.jms.server.management.jmx.impl;

import java.util.Map;

import javax.management.MBeanInfo;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSQueueControl;

/**
 * A ReplicationAwareJMSQueueControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareJMSQueueControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         JMSQueueControlMBean
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ReplicationAwareJMSQueueControlWrapper.class);

   // Attributes ----------------------------------------------------

   private final JMSQueueControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareJMSQueueControlWrapper(final JMSQueueControl localControl,
                                                 final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.JMS_QUEUE + localControl.getName(), JMSQueueControlMBean.class, replicationInvoker);
      this.localControl = localControl;
   }

   // JMSQueueControlMBean implementation ---------------------------

   public int getConsumerCount()
   {
      return localControl.getConsumerCount();
   }

   public String getDeadLetterAddress()
   {
      return localControl.getDeadLetterAddress();
   }
   
   public void setDeadLetterAddress(String deadLetterAddress) throws Exception
   {
      replicationAwareInvoke("setDeadLetterAddress", deadLetterAddress);
   }

   public int getDeliveringCount()
   {
      return localControl.getDeliveringCount();
   }

   public String getExpiryAddress()
   {
      return localControl.getExpiryAddress();
   }

   public int getMessageCount()
   {
      return localControl.getMessageCount();
   }

   public int getMessagesAdded()
   {
      return localControl.getMessagesAdded();
   }

   public String getName()
   {
      return localControl.getName();
   }

   public long getScheduledCount()
   {
      return localControl.getScheduledCount();
   }

   public boolean isDurable()
   {
      return localControl.isDurable();
   }

   public boolean isTemporary()
   {
      return localControl.isTemporary();
   }

   public Map<String, Object>[] listAllMessages() throws Exception
   {
      return localControl.listAllMessages();
   }

   public Object[] listMessageCounter()
   {
      return localControl.listMessageCounter();
   }

   public String listMessageCounterAsHTML()
   {
      return localControl.listMessageCounterAsHTML();
   }

   public Object[] listMessageCounterHistory() throws Exception
   {
      return localControl.listMessageCounterHistory();
   }

   public String listMessageCounterHistoryAsHTML()
   {
      return localControl.listMessageCounterHistoryAsHTML();
   }

   public Map<String, Object>[] listMessages(String filter) throws Exception
   {
      return localControl.listMessages(filter);
   }
   
   public int countMessages(final String filter) throws Exception
   {
      return localControl.countMessages(filter);
   }

   public String getAddress()
   {
      return localControl.getAddress();
   }

   public String getJNDIBinding()
   {
      return localControl.getJNDIBinding();
   }

   public boolean changeMessagePriority(final String messageID, int newPriority) throws Exception
   {
      return (Boolean)replicationAwareInvoke("changeMessagePriority", messageID, newPriority);
   }

   public boolean expireMessage(final String messageID) throws Exception
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

   public boolean moveMessage(final String messageID, final String otherQueueName) throws Exception
   {
      return (Boolean)replicationAwareInvoke("moveMessage", messageID, otherQueueName);
   }

   public int removeMatchingMessages(final String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("removeMatchingMessages", filter);
   }

   public boolean removeMessage(final String messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("removeMessage", messageID);
   }

   public boolean sendMessageToDLQ(final String messageID) throws Exception
   {
      return (Boolean)replicationAwareInvoke("sendMessageToDLQ", messageID);
   }

   public void setExpiryAddress(final String expiryAddress) throws Exception
   {
      replicationAwareInvoke("setExpiryAddress", expiryAddress);
   }

   public int removeAllMessages() throws Exception
   {
      return (Integer)replicationAwareInvoke("removeAllMessages");
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
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSQueueControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
