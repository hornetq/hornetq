/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.deployers.impl;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.QueueDeployer;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * A QueueDeployerTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public class QueueDeployerTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private FakeServerControl serverControl;

   private QueueDeployer deployer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testParseQueueConfigurationFromAddressSettings() throws Exception
   {
      String xml = "<configuration xmlns='urn:jboss:messaging'>" + "   <queues>"
                   + "      <queue name='foo'>"
                   + "         <address>bar</address>"
                   + "         <filter string='speed > 88' />"
                   + "         <durable>false</durable>"
                   + "      </queue>"
                   + "   </queues>"
                   + "</configuration>";

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList queueNodes = rootNode.getElementsByTagName("queue");
      assertEquals(1, queueNodes.getLength());
      deployer.deploy(queueNodes.item(0));

      assertEquals(1, serverControl.configs.size());

      QueueConfiguration queueConfiguration = serverControl.configs.get(0);
      assertEquals("foo", queueConfiguration.getName());
      assertEquals("bar", queueConfiguration.getAddress());
      assertEquals("speed > 88", queueConfiguration.getFilterString());
      assertEquals(false, queueConfiguration.isDurable());
   }

   public void testParseQueueConfigurationFromJBMConfiguration() throws Exception
   {
      String xml = "<configuration xmlns='urn:jboss:messaging'> " + "<queues>"
                   + "   <queue name='foo'>"
                   + "      <address>bar</address>"
                   + "      <filter string='speed > 88' />"
                   + "      <durable>false</durable>"
                   + "   </queue>"
                   + "   <queue name='foo2'>"
                   + "      <address>bar2</address>"
                   + "      <filter string='speed > 88' />"
                   + "      <durable>true</durable>"
                   + "   </queue>"
                   + "</queues>"
                   + "</configuration>";

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList queueNodes = rootNode.getElementsByTagName("queue");
      assertEquals(2, queueNodes.getLength());

      deployer.deploy(queueNodes.item(0));
      deployer.deploy(queueNodes.item(1));

      assertEquals(2, serverControl.configs.size());
      assertEquals("foo", serverControl.configs.get(0).getName());
      assertEquals("foo2", serverControl.configs.get(1).getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      serverControl = new FakeServerControl();
      deployer = new QueueDeployer(deploymentManager, serverControl);
   }

   protected void tearDown() throws Exception
   {
      deployer = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class FakeServerControl implements MessagingServerControlMBean
   {

      public int getThreadPoolMaxSize()
      {

         return 0;
      }

      public boolean closeConnectionsForAddress(String ipAddress) throws Exception
      {

         return false;
      }

      public boolean commitPreparedTransaction(String transactionAsBase64) throws Exception
      {

         return false;
      }

      public void createQueue(String address, String name, String filter, boolean durable) throws Exception
      {

      }

      public void createQueue(String address, String name) throws Exception
      {

      }

      List<QueueConfiguration> configs = new ArrayList<QueueConfiguration>();

      public void deployQueue(String address, String name, String filter, boolean durable) throws Exception
      {
         QueueConfiguration config = new QueueConfiguration(address, name, filter, durable);

         configs.add(config);
      }

      public void deployQueue(String address, String name, String filterString) throws Exception
      {

      }

      public void destroyQueue(String name) throws Exception
      {

      }

      public void disableMessageCounters() throws Exception
      {

      }

      public void enableMessageCounters() throws Exception
      {

      }

      public String getBackupConnectorName()
      {

         return null;
      }

      public String getBindingsDirectory()
      {

         return null;
      }

      public Configuration getConfiguration()
      {

         return null;
      }

      public int getConnectionCount()
      {

         return 0;
      }

      public long getConnectionScanPeriod()
      {

         return 0;
      }

      public long getConnectionTTLOverride()
      {

         return 0;
      }

      public Object[] getConnectors() throws Exception
      {

         return null;
      }

      public int getIDCacheSize()
      {

         return 0;
      }

      public String[] getInterceptorClassNames()
      {

         return null;
      }

      public int getJournalBufferReuseSize()
      {

         return 0;
      }

      public String getJournalDirectory()
      {

         return null;
      }

      public int getJournalFileSize()
      {

         return 0;
      }

      public int getJournalMaxAIO()
      {

         return 0;
      }

      public int getJournalMinFiles()
      {

         return 0;
      }

      public String getJournalType()
      {

         return null;
      }

      public String getLargeMessagesDirectory()
      {

         return null;
      }

      public String getManagementAddress()
      {

         return null;
      }

      public String getManagementNotificationAddress()
      {

         return null;
      }

      public long getManagementRequestTimeout()
      {

         return 0;
      }

      public int getMessageCounterMaxDayCount()
      {

         return 0;
      }

      public long getMessageCounterSamplePeriod()
      {

         return 0;
      }

      public long getMessageExpiryScanPeriod()
      {

         return 0;
      }

      public long getMessageExpiryThreadPriority()
      {

         return 0;
      }

      public String getPagingDirectory()
      {

         return null;
      }

      public int getGlobalPageSize()
      {

         return 0;
      }

      public long getPagingMaxGlobalSizeBytes()
      {

         return 0;
      }

      public long getQueueActivationTimeout()
      {

         return 0;
      }

      public int getScheduledThreadPoolMaxSize()
      {

         return 0;
      }

      public long getSecurityInvalidationInterval()
      {

         return 0;
      }

      public long getTransactionTimeout()
      {

         return 0;
      }

      public long getTransactionTimeoutScanPeriod()
      {

         return 0;
      }

      public String getVersion()
      {

         return null;
      }

      public boolean isBackup()
      {

         return false;
      }

      public boolean isClustered()
      {

         return false;
      }

      public boolean isCreateBindingsDir()
      {

         return false;
      }

      public boolean isCreateJournalDir()
      {

         return false;
      }

      public boolean isJournalSyncNonTransactional()
      {

         return false;
      }

      public boolean isJournalSyncTransactional()
      {

         return false;
      }

      public boolean isMessageCounterEnabled()
      {

         return false;
      }

      public boolean isPersistDeliveryCountBeforeDelivery()
      {

         return false;
      }

      public boolean isPersistIDCache()
      {

         return false;
      }

      public boolean isSecurityEnabled()
      {

         return false;
      }

      public boolean isStarted()
      {

         return false;
      }

      public boolean isWildcardRoutingEnabled()
      {

         return false;
      }

      public String[] listConnectionIDs() throws Exception
      {

         return null;
      }

      public String[] listPreparedTransactions() throws Exception
      {

         return null;
      }

      public String[] listRemoteAddresses() throws Exception
      {

         return null;
      }

      public String[] listRemoteAddresses(String ipAddress) throws Exception
      {

         return null;
      }

      public String[] listSessions(String connectionID) throws Exception
      {

         return null;
      }

      public void resetAllMessageCounterHistories() throws Exception
      {

      }

      public void resetAllMessageCounters() throws Exception
      {

      }

      public boolean rollbackPreparedTransaction(String transactionAsBase64) throws Exception
      {

         return false;
      }

      public void sendQueueInfoToQueue(String queueName, String address) throws Exception
      {

      }

      public void setMessageCounterMaxDayCount(int count) throws Exception
      {

      }

      public void setMessageCounterSamplePeriod(long newPeriod) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.management.MessagingServerControlMBean#getAIOBufferSize()
       */
      public int getAIOBufferSize()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.management.MessagingServerControlMBean#getAIOBufferTimeout()
       */
      public int getAIOBufferTimeout()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.management.MessagingServerControlMBean#getAIOFlushOnSync()
       */
      public boolean isAIOFlushOnSync()
      {

         return false;
      }

   }

}
