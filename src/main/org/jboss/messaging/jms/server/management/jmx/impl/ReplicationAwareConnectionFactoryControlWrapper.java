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

import java.util.List;

import javax.management.MBeanInfo;
import javax.management.ObjectName;

import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareStandardMBeanWrapper;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControlMBean;
import org.jboss.messaging.jms.server.management.impl.ConnectionFactoryControl;

/**
 * A ReplicationAwareConnectionFactoryControlWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareConnectionFactoryControlWrapper extends ReplicationAwareStandardMBeanWrapper implements
         ConnectionFactoryControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ConnectionFactoryControl localControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationAwareConnectionFactoryControlWrapper(final ObjectName objectName,
                                                          final ConnectionFactoryControl localControl,
                                                          final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(objectName, ConnectionFactoryControlMBean.class, replicationInvoker);
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

   public long getPingPeriod()
   {
      return localControl.getPingPeriod();
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
                           MBeanInfoHelper.getMBeanOperationsInfo(ConnectionFactoryControlMBean.class),
                           info.getNotifications());
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
