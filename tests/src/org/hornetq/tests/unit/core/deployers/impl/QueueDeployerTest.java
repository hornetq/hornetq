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
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.Parameter;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.QueueDeployer;
import org.hornetq.core.security.Role;
import org.hornetq.core.settings.impl.AddressSettings;
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
      Assert.assertEquals(1, queueNodes.getLength());
      deployer.deploy(queueNodes.item(0));

      Assert.assertEquals(1, serverControl.configs.size());

      CoreQueueConfiguration queueConfiguration = serverControl.configs.get(0);
      Assert.assertEquals("foo", queueConfiguration.getName());
      Assert.assertEquals("bar", queueConfiguration.getAddress());
      Assert.assertEquals("speed > 88", queueConfiguration.getFilterString());
      Assert.assertEquals(false, queueConfiguration.isDurable());
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
      Assert.assertEquals(2, queueNodes.getLength());

      deployer.deploy(queueNodes.item(0));
      deployer.deploy(queueNodes.item(1));

      Assert.assertEquals(2, serverControl.configs.size());
      Assert.assertEquals("foo", serverControl.configs.get(0).getName());
      Assert.assertEquals("foo2", serverControl.configs.get(1).getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      serverControl = new FakeServerControl();
      deployer = new QueueDeployer(deploymentManager, serverControl);
   }

   @Override
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

      public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
      {

         return false;
      }

      public boolean commitPreparedTransaction(final String transactionAsBase64) throws Exception
      {

         return false;
      }

      public void createQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
      {

      }

      public void createQueue(final String address, final String name) throws Exception
      {

      }

      public void createQueue(final String address, final String name, final boolean durable) throws Exception
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

      List<CoreQueueConfiguration> configs = new ArrayList<CoreQueueConfiguration>();

      public void deployQueue(final String address, final String name, final String filter, final boolean durable) throws Exception
      {
         CoreQueueConfiguration config = new CoreQueueConfiguration(address, name, filter, durable);

         configs.add(config);
      }

      public void deployQueue(final String address, final String name, final String filterString) throws Exception
      {

      }

      public void destroyQueue(final String name) throws Exception
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

      public int getJournalMaxIO()
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

      public boolean isAsyncConnectionExecutionEnabled()
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

      public String[] listRemoteAddresses(final String ipAddress) throws Exception
      {

         return null;
      }

      public String[] listSessions(final String connectionID) throws Exception
      {

         return null;
      }

      public void resetAllMessageCounterHistories() throws Exception
      {

      }

      public void resetAllMessageCounters() throws Exception
      {

      }

      public boolean rollbackPreparedTransaction(final String transactionAsBase64) throws Exception
      {

         return false;
      }

      public void sendQueueInfoToQueue(final String queueName, final String address) throws Exception
      {

      }

      public void setMessageCounterMaxDayCount(final int count) throws Exception
      {

      }

      public void setMessageCounterSamplePeriod(final long newPeriod) throws Exception
      {

      }

      public int getJournalBufferSize()
      {

         return 0;
      }

      public int getJournalBufferTimeout()
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
      
      public void addSecuritySettings(String addressMatch,
                                      String sendRoles,
                                      String consumeRoles,
                                      String createDurableQueueRoles,
                                      String deleteDurableQueueRoles,
                                      String createTempQueueRoles,
                                      String deleteTempQueueRoles,
                                      String manageRoles) throws Exception
      {
      }
      
      public void removeSecuritySettings(String addressMatch) throws Exception
      {
      }

      public Set<Role> getSecuritySettings(String addressMatch) throws Exception
      {
         return null;
      }

      public Object[] getRoles(String addressMatch) throws Exception
      {
         return null;
      }
      
      public String getRolesAsJSON(String addressMatch) throws Exception
      {
         return null;
      }

      public void addAddressSettings(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch, @Parameter(desc = "the dead letter address setting", name = "DLA") String DLA, @Parameter(desc = "the expiry address setting", name = "expiryAddress") String expiryAddress, @Parameter(desc = "are any queues created for this address a last value queue", name = "lastValueQueue") boolean lastValueQueue, @Parameter(desc = "the delivery attempts", name = "deliveryAttempts") int deliveryAttempts, @Parameter(desc = "the max size in bytes", name = "maxSizeBytes") long maxSizeBytes, @Parameter(desc = "the page size in bytes", name = "pageSizeBytes") int pageSizeBytes, @Parameter(desc = "the redelivery delay", name = "redeliveryDelay") long redeliveryDelay, @Parameter(desc = "the redistribution delay", name = "redistributionDelay") long redistributionDelay, @Parameter(desc = "do we send to the DLA when there is no where to route the message", name = "sendToDLAOnNoRoute") boolean sendToDLAOnNoRoute, @Parameter(desc = "the ploicy to use when the address is full", name = "addressFullMessagePolicy") String addressFullMessagePolicy) throws Exception
      {

      }

      public AddressSettings getAddressSettings(String address)
      {
         return null;
      }

      public void removeAddressSettings(String addressMatch)
      {

      }

      public String getAddressSettingsAsJSON(@Parameter(desc = "an address match", name = "addressMatch") String addressMatch) throws Exception
      {
         return null;  
      }
   }

}
