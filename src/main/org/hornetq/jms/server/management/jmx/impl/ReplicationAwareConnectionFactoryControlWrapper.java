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

import java.util.List;

import javax.management.MBeanInfo;

import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.hornetq.jms.server.management.ConnectionFactoryControl;
import org.hornetq.jms.server.management.impl.ConnectionFactoryControlImpl;

/**
 * A ReplicationAwareConnectionFactoryControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareConnectionFactoryControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         ConnectionFactoryControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ConnectionFactoryControlImpl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareConnectionFactoryControlWrapper(final ConnectionFactoryControlImpl localControl,
                                                          final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(ResourceNames.JMS_CONNECTION_FACTORY + localControl.getName(), ConnectionFactoryControl.class, replicationInvoker);
      this.localControl = localControl;
   }

   // ConnectionFactoryControlMBean implementation ---------------------------

   public String getName()
   {
      return localControl.getName();
   }

   public List<String> getBindings()
   {
      return localControl.getBindings();
   }

   public long getCallTimeout()
   {
      return localControl.getCallTimeout();
   }

   public String getClientID()
   {
      return localControl.getClientID();
   }

   public int getConsumerMaxRate()
   {
      return localControl.getConsumerMaxRate();
   }

   public int getConsumerWindowSize()
   {
      return localControl.getConsumerWindowSize();
   }

   public int getProducerMaxRate()
   {
      return localControl.getProducerMaxRate();
   }

   public int getProducerWindowSize()
   {
      return localControl.getProducerWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return localControl.getDupsOKBatchSize();
   }

   public long getClientFailureCheckPeriod()
   {
      return localControl.getClientFailureCheckPeriod();
   }

   public boolean isBlockOnAcknowledge()
   {
      return localControl.isBlockOnAcknowledge();
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return localControl.isBlockOnNonPersistentSend();
   }

   public boolean isBlockOnPersistentSend()
   {
      return localControl.isBlockOnPersistentSend();
   }

   public boolean isPreAcknowledge()
   {
      return localControl.isPreAcknowledge();
   }

   public long getConnectionTTL()
   {
      return localControl.getConnectionTTL();
   }

   public int getMaxConnections()
   {
      return localControl.getMaxConnections();
   }

   public int getReconnectAttempts()
   {
      return localControl.getReconnectAttempts();
   }

   public boolean isFailoverOnNodeShutdown()
   {
      return localControl.isFailoverOnNodeShutdown();
   }

   public long getMinLargeMessageSize()
   {
      return localControl.getMinLargeMessageSize();
   }

   public long getRetryInterval()
   {
      return localControl.getRetryInterval();
   }

   public double getRetryIntervalMultiplier()
   {
      return localControl.getRetryIntervalMultiplier();
   }

   public long getTransactionBatchSize()
   {
      return localControl.getTransactionBatchSize();
   }

   public boolean isAutoGroup()
   {
      return localControl.isAutoGroup();
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
                           MBeanInfoHelper.getMBeanOperationsInfo(ConnectionFactoryControl.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
