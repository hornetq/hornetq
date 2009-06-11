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

import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.TopicControl;
import org.jboss.messaging.jms.server.management.impl.TopicControlImpl;

/**
 * A ReplicationAwareTopicControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareTopicControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         TopicControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final TopicControlImpl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareTopicControlWrapper(final TopicControlImpl localControl,
                                              final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.JMS_TOPIC + localControl.getName(), TopicControl.class, replicationInvoker);
      
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

   public int getDurableMessageCount()
   {
      return localControl.getDurableMessageCount();
   }

   public int getDurableSubscriptionCount()
   {
      return localControl.getDurableSubscriptionCount();
   }

   public int getNonDurableMessageCount()
   {
      return localControl.getNonDurableMessageCount();
   }

   public int getNonDurableSubscriptionCount()
   {
      return localControl.getNonDurableSubscriptionCount();
   }

   public int getSubscriptionCount()
   {
      return localControl.getSubscriptionCount();
   }

   public Object[] listAllSubscriptions()
   {
      return localControl.listAllSubscriptions();
   }
   
   public String listAllSubscriptionsAsJSON() throws Exception
   {
      return localControl.listAllSubscriptionsAsJSON();
   }

   public Object[] listDurableSubscriptions()
   {
      return localControl.listDurableSubscriptions();
   }
   
   public String listDurableSubscriptionsAsJSON() throws Exception
   {
      return localControl.listDurableSubscriptionsAsJSON();
   }

   public Map<String, Object>[] listMessagesForSubscription(final String queueName) throws Exception
   {
      return localControl.listMessagesForSubscription(queueName);
   }
   
   public String listMessagesForSubscriptionAsJSON(String queueName) throws Exception
   {
      return localControl.listMessagesForSubscriptionAsJSON(queueName);
   }
   
   public int countMessagesForSubscription(final String clientID, final String subscriptionName, final String filterStr) throws Exception
   {
      return localControl.countMessagesForSubscription(clientID, subscriptionName, filterStr);
   }

   public Object[] listNonDurableSubscriptions()
   {
      return localControl.listNonDurableSubscriptions();
   }
   
   public String listNonDurableSubscriptionsAsJSON() throws Exception
   {
      return localControl.listNonDurableSubscriptionsAsJSON();
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
                           MBeanInfoHelper.getMBeanOperationsInfo(TopicControl.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
