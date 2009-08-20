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

package org.hornetq.jms.server.management.jmx.impl;

import java.util.Map;

import javax.management.MBeanInfo;

import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.hornetq.jms.server.management.TopicControl;
import org.hornetq.jms.server.management.impl.TopicControlImpl;

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

   public int removeMessages(String filter) throws Exception
   {
      return (Integer)replicationAwareInvoke("removeMessages", filter);
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
