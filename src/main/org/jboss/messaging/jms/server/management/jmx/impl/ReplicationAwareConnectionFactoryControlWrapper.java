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

   public int getDefaultConsumerMaxRate()
   {
      return localControl.getDefaultConsumerMaxRate();
   }

   public int getDefaultConsumerWindowSize()
   {
      return localControl.getDefaultConsumerWindowSize();
   }

   public int getDefaultProducerMaxRate()
   {
      return localControl.getDefaultProducerMaxRate();
   }

   public int getDefaultProducerWindowSize()
   {
      return localControl.getDefaultProducerWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return localControl.getDupsOKBatchSize();
   }

   public long getPingPeriod()
   {
      return localControl.getPingPeriod();
   }

   public boolean isDefaultBlockOnAcknowledge()
   {
      return localControl.isDefaultBlockOnAcknowledge();
   }

   public boolean isDefaultBlockOnNonPersistentSend()
   {
      return localControl.isDefaultBlockOnNonPersistentSend();
   }

   public boolean isDefaultBlockOnPersistentSend()
   {
      return localControl.isDefaultBlockOnPersistentSend();
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
