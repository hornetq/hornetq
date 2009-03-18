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

import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.jms.server.management.impl.TopicControl;

/**
 * A ReplicationAwareTopicControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareTopicControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         TopicControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final TopicControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareTopicControlWrapper(final ObjectName objectName,
                                              final TopicControl localControl,
                                              final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(objectName, TopicControlMBean.class, replicationInvoker);
      
      this.localControl = localControl;
   }

   // TopicControlMBean implementation ---------------------------

   public void dropAllSubscriptions() throws Exception
   {
      replicationAwareInvoke("dropAllSubscriptions");
   }

   public void dropDurableSubscription(final String clientID, final String subscriptionName) throws Exception
   {
      replicationAwareInvoke("dropDurableSubscription", clientID, subscriptionName);
   }

   public int getDurableMessagesCount()
   {
      return localControl.getDurableMessagesCount();
   }

   public int getDurableSubcriptionsCount()
   {
      return localControl.getDurableSubcriptionsCount();
   }

   public int getNonDurableMessagesCount()
   {
      return localControl.getNonDurableMessagesCount();
   }

   public int getNonDurableSubcriptionsCount()
   {
      return localControl.getNonDurableSubcriptionsCount();
   }

   public int getSubcriptionsCount()
   {
      return localControl.getSubcriptionsCount();
   }

   public TabularData listAllSubscriptions()
   {
      return localControl.listAllSubscriptions();
   }

   public TabularData listDurableSubscriptions()
   {
      return localControl.listDurableSubscriptions();
   }

   public TabularData listMessagesForSubscription(final String queueName) throws Exception
   {
      return localControl.listMessagesForSubscription(queueName);
   }
   
   public int countMessagesForSubscription(final String clientID, final String subscriptionName, final String filterStr) throws Exception
   {
      return localControl.countMessagesForSubscription(clientID, subscriptionName, filterStr);
   }

   public TabularData listNonDurableSubscriptions()
   {
      return localControl.listNonDurableSubscriptions();
   }

   public String getAddress()
   {
      return localControl.getAddress();
   }

   public String getJNDIBinding()
   {
      return localControl.getJNDIBinding();
   }

   public int getMessageCount() throws Exception
   {
      return localControl.getMessageCount();
   }

   public String getName()
   {
      return localControl.getName();
   }

   public boolean isTemporary()
   {
      return localControl.isTemporary();
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
                           MBeanInfoHelper.getMBeanOperationsInfo(TopicControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
