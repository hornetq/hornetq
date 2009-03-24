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

package org.jboss.messaging.jms.server.management.impl;

import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControlMBean;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectionFactoryControl extends StandardMBean implements ConnectionFactoryControlMBean
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossConnectionFactory cf;

   private final List<String> bindings;

   private final String name;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionFactoryControl(final JBossConnectionFactory cf, final String name, final List<String> bindings) throws NotCompliantMBeanException
   {
      super(ConnectionFactoryControlMBean.class);
      this.cf = cf;
      this.name = name;
      this.bindings = bindings;
   }

   // Public --------------------------------------------------------

   // ManagedConnectionFactoryMBean implementation ------------------

   public List<String> getBindings()
   {
      return bindings;
   }

   public String getClientID()
   {
      return cf.getClientID();
   }

   public long getPingPeriod()
   {
      return cf.getPingPeriod();
   }

   public long getCallTimeout()
   {
      return cf.getCallTimeout();
   }

   public int getConsumerMaxRate()
   {
      return cf.getConsumerMaxRate();
   }

   public int getConsumerWindowSize()
   {
      return cf.getConsumerWindowSize();
   }

   public int getProducerMaxRate()
   {
      return cf.getProducerMaxRate();
   }

   public int getProducerWindowSize()
   {
      return cf.getProducerWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return cf.getDupsOKBatchSize();
   }

   public boolean isBlockOnAcknowledge()
   {
      return cf.isBlockOnAcknowledge();
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return cf.isBlockOnNonPersistentSend();
   }

   public boolean isBlockOnPersistentSend()
   {
      return cf.isBlockOnPersistentSend();
   }

   public boolean isPreAcknowledge()
   {
      return cf.isPreAcknowledge();
   }

   public String getName()
   {
      return name;
   }

   public long getConnectionTTL()
   {
      return cf.getConnectionTTL();
   }

   public int getMaxConnections()
   {
      return cf.getMaxConnections();
   }

   public int getInitialConnectAttempts()
   {
      return cf.getInitialConnectAttempts();
   }

   public int getReconnectAttempts()
   {
      return cf.getReconnectAttempts();
   }

   public long getMinLargeMessageSize()
   {
      return cf.getMinLargeMessageSize();
   }

   public long getRetryInterval()
   {
      return cf.getRetryInterval();
   }

   public double getRetryIntervalMultiplier()
   {
      return cf.getRetryIntervalMultiplier();
   }

   public long getTransactionBatchSize()
   {
      return cf.getTransactionBatchSize();
   }

   public boolean isAutoGroup()
   {
      return cf.isAutoGroup();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
