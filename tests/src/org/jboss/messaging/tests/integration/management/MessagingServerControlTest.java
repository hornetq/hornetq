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

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.MessagingServerControl;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.QueueControl;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterManagerImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.json.JSONArray;
import org.jboss.messaging.utils.json.JSONObject;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 * 
 * Created 26 nov. 2008 14:18:48
 *
 *
 */
public class MessagingServerControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private Configuration conf;

   private TransportConfiguration connectorConfig;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {
      MessagingServerControl serverControl = createManagementControl();

      assertEquals(server.getVersion().getFullVersion(), serverControl.getVersion());

      assertEquals(conf.isClustered(), serverControl.isClustered());     
      assertEquals(conf.isPersistDeliveryCountBeforeDelivery(), serverControl.isPersistDeliveryCountBeforeDelivery());
      assertEquals(conf.isBackup(), serverControl.isBackup());
      assertEquals(conf.getQueueActivationTimeout(), serverControl.getQueueActivationTimeout());
      assertEquals(conf.getScheduledThreadPoolMaxSize(), serverControl.getScheduledThreadPoolMaxSize());
      assertEquals(conf.getThreadPoolMaxSize(), serverControl.getThreadPoolMaxSize());
      assertEquals(conf.getSecurityInvalidationInterval(), serverControl.getSecurityInvalidationInterval());
      assertEquals(conf.isSecurityEnabled(), serverControl.isSecurityEnabled());
      assertEquals(conf.getInterceptorClassNames().size(), serverControl.getInterceptorClassNames().length);     
      assertEquals(conf.getConnectionTTLOverride(), serverControl.getConnectionTTLOverride());
      assertEquals(conf.getBackupConnectorName(), serverControl.getBackupConnectorName());
      assertEquals(conf.getManagementAddress().toString(), serverControl.getManagementAddress());
      assertEquals(conf.getManagementNotificationAddress().toString(), serverControl.getManagementNotificationAddress());
      assertEquals(conf.getManagementRequestTimeout(), serverControl.getManagementRequestTimeout());
      assertEquals(conf.getIDCacheSize(), serverControl.getIDCacheSize());
      assertEquals(conf.isPersistIDCache(), serverControl.isPersistIDCache());
      assertEquals(conf.getBindingsDirectory(), serverControl.getBindingsDirectory());
      assertEquals(conf.getJournalDirectory(), serverControl.getJournalDirectory());
      assertEquals(conf.getJournalType().toString(), serverControl.getJournalType());
      assertEquals(conf.isJournalSyncTransactional(), serverControl.isJournalSyncTransactional());
      assertEquals(conf.isJournalSyncNonTransactional(), serverControl.isJournalSyncNonTransactional());
      assertEquals(conf.getJournalFileSize(), serverControl.getJournalFileSize());
      assertEquals(conf.getJournalMinFiles(), serverControl.getJournalMinFiles());
      assertEquals(conf.getJournalMaxAIO(), serverControl.getJournalMaxAIO());
      assertEquals(conf.getAIOBufferSize(), serverControl.getAIOBufferSize());
      assertEquals(conf.getAIOBufferTimeout(), serverControl.getAIOBufferTimeout());      
      assertEquals(conf.isCreateBindingsDir(), serverControl.isCreateBindingsDir());
      assertEquals(conf.isCreateJournalDir(), serverControl.isCreateJournalDir());      
      assertEquals(conf.getPagingDirectory(), serverControl.getPagingDirectory());
      assertEquals(conf.getLargeMessagesDirectory(), serverControl.getLargeMessagesDirectory());
      assertEquals(conf.isWildcardRoutingEnabled(), serverControl.isWildcardRoutingEnabled());
      assertEquals(conf.getTransactionTimeout(), serverControl.getTransactionTimeout());
      assertEquals(conf.isMessageCounterEnabled(), serverControl.isMessageCounterEnabled());
      assertEquals(conf.getTransactionTimeoutScanPeriod(), serverControl.getTransactionTimeoutScanPeriod());
      assertEquals(conf.getMessageExpiryScanPeriod(), serverControl.getMessageExpiryScanPeriod());
      assertEquals(conf.getMessageExpiryThreadPriority(), serverControl.getMessageExpiryThreadPriority());
   }

   public void testGetConnectors() throws Exception
   {
      MessagingServerControl serverControl = createManagementControl();

      Object[] connectorData = serverControl.getConnectors();
      assertNotNull(connectorData);
      assertEquals(1, connectorData.length);

      Object[] config = (Object[])connectorData[0];           

      assertEquals(connectorConfig.getName(), config[0]);
   }
   
   public void testGetConnectorsAsJSON() throws Exception
   {
      MessagingServerControl serverControl = createManagementControl();

      String jsonString = serverControl.getConnectorsAsJSON();
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      assertEquals(connectorConfig.getName(), data.optString("name"));
      assertEquals(connectorConfig.getFactoryClassName(), data.optString("factoryClassName"));
      assertEquals(connectorConfig.getParams().size(), data.getJSONObject("params").length());
   }

   public void testCreateAndDestroyQueue() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();

      MessagingServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNames.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString());

      checkResource(ObjectNames.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(true, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNames.getQueueObjectName(address, name));
   }

   public void testCreateAndDestroyQueue_2() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = "color = 'green'";
      boolean durable = true;

      MessagingServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNames.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNames.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertEquals(filter, queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNames.getQueueObjectName(address, name));
   }

   public void testCreateAndDestroyQueueWithNullFilter() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString name = RandomUtil.randomSimpleString();
      String filter = null;
      boolean durable = true;

      MessagingServerControl serverControl = createManagementControl();

      checkNoResource(ObjectNames.getQueueObjectName(address, name));

      serverControl.createQueue(address.toString(), name.toString(), filter, durable);

      checkResource(ObjectNames.getQueueObjectName(address, name));
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, name, mbeanServer);
      assertEquals(address.toString(), queueControl.getAddress());
      assertEquals(name.toString(), queueControl.getName());
      assertNull(queueControl.getFilter());
      assertEquals(durable, queueControl.isDurable());
      assertEquals(false, queueControl.isTemporary());

      serverControl.destroyQueue(name.toString());

      checkNoResource(ObjectNames.getQueueObjectName(address, name));
   }

   public void testMessageCounterMaxDayCount() throws Exception
   {
      MessagingServerControl serverControl = createManagementControl();

      assertEquals(MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT, serverControl.getMessageCounterMaxDayCount());

      int newCount = 100;
      serverControl.setMessageCounterMaxDayCount(newCount);

      assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());

      try
      {
         serverControl.setMessageCounterMaxDayCount(-1);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterMaxDayCount(0);
         fail();
      }
      catch (Exception e)
      {
      }

      assertEquals(newCount, serverControl.getMessageCounterMaxDayCount());
   }

   public void testGetMessageCounterSamplePeriod() throws Exception
   {
      MessagingServerControl serverControl = createManagementControl();

      assertEquals(MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD, serverControl.getMessageCounterSamplePeriod());

      long newSample = 20000;
      serverControl.setMessageCounterSamplePeriod(newSample);

      assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());

      try
      {
         serverControl.setMessageCounterSamplePeriod(-1);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(0);
         fail();
      }
      catch (Exception e)
      {
      }

      try
      {
         serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD - 1);
         fail();
      }
      catch (Exception e)
      {
      }

      assertEquals(newSample, serverControl.getMessageCounterSamplePeriod());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(randomString(), randomBoolean());
      connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          params,
                                                                          randomString());

      conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      
      server = Messaging.newMessagingServer(conf, mbeanServer, false);
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null)
      {
         server.stop();
      }
      
      server = null;
      
      connectorConfig = null;

      super.tearDown();
   }

   protected MessagingServerControl createManagementControl() throws Exception
   {
      return ManagementControlHelper.createMessagingServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
