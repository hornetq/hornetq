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

package org.hornetq.jms.management.impl;

import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.Parameter;
import org.hornetq.api.jms.management.ConnectionFactoryControl;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSConnectionFactoryControlImpl extends StandardMBean implements ConnectionFactoryControl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final HornetQConnectionFactory cf;

   private final String name;
   
   private final JMSServerManager jmsManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSConnectionFactoryControlImpl(final HornetQConnectionFactory cf,
                                          final JMSServerManager jmsManager,
                                          final String name) throws NotCompliantMBeanException
   {
      super(ConnectionFactoryControl.class);
      this.cf = cf;
      this.name = name;
      this.jmsManager = jmsManager;
   }

   // Public --------------------------------------------------------

   // ManagedConnectionFactoryMBean implementation ------------------

   public String[] getJNDIBindings()
   {
      return jmsManager.getJNDIOnConnectionFactory(name);
   }

   public String getClientID()
   {
      return cf.getClientID();
   }

   public long getClientFailureCheckPeriod()
   {
      return cf.getClientFailureCheckPeriod();
   }

   public long getDiscoveryRefreshTimeout()
   {
      return cf.getDiscoveryRefreshTimeout();
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return cf.getConnectionLoadBalancingPolicyClassName();
   }

   public void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout)
   {
      cf.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   public long getDiscoveryInitialWaitTimeout()
   {
      return cf.getDiscoveryInitialWaitTimeout();
   }

   public void setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout)
   {
      cf.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   public void setClientID(String clientID)
   {
      cf.setClientID(clientID);
   }

   public void setDupsOKBatchSize(int dupsOKBatchSize)
   {
      cf.setDupsOKBatchSize(dupsOKBatchSize);
   }

   public void setTransactionBatchSize(int transactionBatchSize)
   {
      cf.setTransactionBatchSize(transactionBatchSize);
   }

   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);
   }

   public void setConnectionTTL(long connectionTTL)
   {
      cf.setConnectionTTL(connectionTTL);
   }

   public void setCallTimeout(long callTimeout)
   {
      cf.setCallTimeout(callTimeout);
   }

   public void setConsumerWindowSize(int consumerWindowSize)
   {
      cf.setConsumerWindowSize(consumerWindowSize);
   }

   public void setConsumerMaxRate(int consumerMaxRate)
   {
      cf.setConsumerMaxRate(consumerMaxRate);
   }

   public void setConfirmationWindowSize(int confirmationWindowSize)
   {
      cf.setConfirmationWindowSize(confirmationWindowSize);
   }

   public void setProducerMaxRate(int producerMaxRate)
   {
      cf.setProducerMaxRate(producerMaxRate);
   }

   public int getProducerWindowSize()
   {
      return cf.getProducerWindowSize();
   }

   public void setProducerWindowSize(int producerWindowSize)
   {
      cf.setProducerWindowSize(producerWindowSize);
   }

   public void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient)
   {
      cf.setCacheLargeMessagesClient(cacheLargeMessagesClient);
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cf.isCacheLargeMessagesClient();
   }

   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      cf.setMinLargeMessageSize(minLargeMessageSize);
   }

   public void setBlockOnNonDurableSend(boolean blockOnNonDurableSend)
   {
      cf.setBlockOnNonDurableSend(blockOnNonDurableSend);
   }

   public void setBlockOnAcknowledge(boolean blockOnAcknowledge)
   {
      cf.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   public void setBlockOnDurableSend(boolean blockOnDurableSend)
   {
      cf.setBlockOnDurableSend(blockOnDurableSend);
   }

   public void setAutoGroup(boolean autoGroup)
   {
      cf.setAutoGroup(autoGroup);
   }

   public void setPreAcknowledge(boolean preAcknowledge)
   {
      cf.setPreAcknowledge(preAcknowledge);
   }

   public void setMaxRetryInterval(long retryInterval)
   {
      cf.setMaxRetryInterval(retryInterval);
   }

   public void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      cf.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   public void setReconnectAttempts(int reconnectAttempts)
   {
      cf.setReconnectAttempts(reconnectAttempts);
   }
   
   public void setFailoverOnInitialConnection(boolean failover)
   {
      cf.setFailoverOnInitialConnection(failover);
   }

   public boolean isUseGlobalPools()
   {
      return cf.isUseGlobalPools();
   }

   public void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize)
   {
      cf.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
   }

   public int getThreadPoolMaxSize()
   {
      return cf.getThreadPoolMaxSize();
   }

   public void setThreadPoolMaxSize(int threadPoolMaxSize)
   {
      cf.setThreadPoolMaxSize(threadPoolMaxSize);
   }

   public int getInitialMessagePacketSize()
   {
      return cf.getInitialMessagePacketSize();
   }

   public void setGroupID(String groupID)
   {
      cf.setGroupID(groupID);
   }

   public String getGroupID()
   {
      return cf.getGroupID();
   }

   public void setInitialMessagePacketSize(int size)
   {
      cf.setInitialMessagePacketSize(size);
   }

   public void setUseGlobalPools(boolean useGlobalPools)
   {
      cf.setUseGlobalPools(useGlobalPools);
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return cf.getScheduledThreadPoolMaxSize();
   }

   public void setRetryInterval(long retryInterval)
   {
      cf.setRetryInterval(retryInterval);
   }

   public long getMaxRetryInterval()
   {
      return cf.getMaxRetryInterval();
   }

   public void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName)
   {
      cf.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
   }

   public TransportConfiguration[] getStaticConnectors()
   {
      return cf.getStaticConnectors();
   }

   public String getLocalBindAddress()
   {
      return cf.getLocalBindAddress();
   }

   public void setLocalBindAddress(String localBindAddress)
   {
      cf.setLocalBindAddress(localBindAddress);
   }

   public String getDiscoveryAddress()
   {
      return cf.getDiscoveryAddress();
   }

   public int getDiscoveryPort()
   {
      return cf.getDiscoveryPort();
   }

   public void addJNDI(@Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndi) throws Exception
   {
       jmsManager.addConnectionFactoryToJNDI(name, jndi);
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

   public int getConfirmationWindowSize()
   {
      return cf.getConfirmationWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return cf.getDupsOKBatchSize();
   }

   public boolean isBlockOnAcknowledge()
   {
      return cf.isBlockOnAcknowledge();
   }

   public boolean isBlockOnNonDurableSend()
   {
      return cf.isBlockOnNonDurableSend();
   }

   public boolean isBlockOnDurableSend()
   {
      return cf.isBlockOnDurableSend();
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

   public int getReconnectAttempts()
   {
      return cf.getReconnectAttempts();
   }
   
   public boolean isFailoverOnInitialConnection()
   {
      return cf.isFailoverOnInitialConnection();
   }

   public int getMinLargeMessageSize()
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

   public int getTransactionBatchSize()
   {
      return cf.getTransactionBatchSize();
   }

   public boolean isAutoGroup()
   {
      return cf.isAutoGroup();
   }

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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
