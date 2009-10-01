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

package org.hornetq.tests.unit.core.deployers.impl;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.QueueDeployer;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.tests.util.UnitTestCase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

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
      String xml = "<configuration xmlns='urn:hornetq'>" + "   <queues>"
                   + "      <queue name='foo'>"
                   + "         <address>bar</address>"
                   + "         <filter string='speed > 88' />"
                   + "         <durable>false</durable>"
                   + "      </queue>"
                   + "   </queues>"
                   + "</configuration>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);
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

   public void testParseQueueConfigurationFromHornetQConfiguration() throws Exception
   {
      String xml = "<configuration xmlns='urn:hornetq'> " + "<queues>"
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

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);
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

   private class FakeServerControl implements HornetQServerControl
   {
      public boolean isSharedStore()
      {
         return false;
      }

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
      
      public void createQueue(String address, String name, boolean durable) throws Exception
      {

      }

      public String[] getAddressNames()
      {
         return null;
      }
      
      public String[] getQueueNames()
      {
         return null;
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
      
      public long getConnectionTTLOverride()
      {

         return 0;
      }

      public Object[] getConnectors() throws Exception
      {

         return null;
      }
      
      public String getConnectorsAsJSON() throws Exception
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
      
      public String[] listHeuristicCommittedTransactions() throws Exception
      {
         return null;
      }
      
      public String[] listHeuristicRolledBackTransactions() throws Exception
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

      public int getAIOBufferSize()
      {

         return 0;
      }

      public int getAIOBufferTimeout()
      {
         return 0;
      }

      public int getJournalCompactMinFiles()
      {
         return 0;
      }

      public int getJournalCompactPercentage()
      {
         return 0;
      }

      public boolean isPersistenceEnabled()
      {
         return false;
      }

   }

}
