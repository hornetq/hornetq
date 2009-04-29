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

import java.util.ArrayList;
import java.util.List;

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.QueueDeployer;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.tests.util.UnitTestCase;
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
      String xml = "<settings xmlns='urn:jboss:messaging'>"
                 + "   <queue name='foo'>"
                 + "      <address>bar</address>"
                 + "      <filter string='speed > 88' />"
                 + "      <durable>false</durable>" 
                 + "   </queue>"
                 + "</settings>";
      
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
      String xml = "<deployment xmlns='urn:jboss:messaging'> " 
                 + "<configuration>"
                 + "<acceptors>"
                 + "<acceptor><factory-class>FooAcceptor</factory-class></acceptor>"
                 + "</acceptors>"
                 + "<queues>"
                 + "   <queue name='foo'>"
                 + "      <address>bar</address>"
                 + "      <filter string='speed > 88' />"
                 + "      <durable>false</durable>" 
                 + "   </queue>"
                 + "</queues>"
                 + "</configuration>"
                 + "<settings>"
                 + "   <queue name='foo2'>"
                 + "      <address>bar2</address>"
                 + "      <filter string='speed > 88' />"
                 + "      <durable>true</durable>" 
                 + "   </queue>"
                 + "</settings>"
                 + "</deployment>";
      
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
         // TODO Auto-generated method stub
         return 0;
      }

      public boolean closeConnectionsForAddress(String ipAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean commitPreparedTransaction(String transactionAsBase64) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void createQueue(String address, String name, String filter, boolean durable) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void createQueue(String address, String name) throws Exception
      {
         // TODO Auto-generated method stub
         
      }
      
      List<QueueConfiguration> configs = new ArrayList<QueueConfiguration>();

      public void deployQueue(String address, String name, String filter, boolean durable) throws Exception
      {
         QueueConfiguration config = new QueueConfiguration(address, name, filter, durable);
         
         configs.add(config);
      }

      public void deployQueue(String address, String name) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void destroyQueue(String name) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void disableMessageCounters() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void enableMessageCounters() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public String getBackupConnectorName()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getBindingsDirectory()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Configuration getConfiguration()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getConnectionCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getConnectionScanPeriod()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getConnectionTTLOverride()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public TabularData getConnectors() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getIDCacheSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public List<String> getInterceptorClassNames()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getJournalBufferReuseSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public String getJournalDirectory()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getJournalFileSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getJournalMaxAIO()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getJournalMinFiles()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public String getJournalType()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getLargeMessagesDirectory()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getManagementAddress()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getManagementNotificationAddress()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getManagementRequestTimeout()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getMessageCounterMaxDayCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getMessageCounterSamplePeriod()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getMessageExpiryScanPeriod()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getMessageExpiryThreadPriority()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public String getPagingDirectory()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getPagingGlobalWatermarkSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getPagingMaxGlobalSizeBytes()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getQueueActivationTimeout()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getScheduledThreadPoolMaxSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getSecurityInvalidationInterval()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getTransactionTimeout()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getTransactionTimeoutScanPeriod()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public String getVersion()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean isBackup()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isClustered()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isCreateBindingsDir()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isCreateJournalDir()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isJournalSyncNonTransactional()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isJournalSyncTransactional()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isMessageCounterEnabled()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isPersistDeliveryCountBeforeDelivery()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isPersistIDCache()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isSecurityEnabled()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isStarted()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isWildcardRoutingEnabled()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public String[] listConnectionIDs() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listPreparedTransactions() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listRemoteAddresses() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listRemoteAddresses(String ipAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listSessions(String connectionID) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void resetAllMessageCounterHistories() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void resetAllMessageCounters() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public boolean rollbackPreparedTransaction(String transactionAsBase64) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void sendQueueInfoToQueue(String queueName, String address) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void setMessageCounterMaxDayCount(int count) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void setMessageCounterSamplePeriod(long newPeriod) throws Exception
      {
         // TODO Auto-generated method stub
         
      }
      
   }

}
