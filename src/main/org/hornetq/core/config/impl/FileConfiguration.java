/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.core.config.impl;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.Pair;
import org.hornetq.api.SimpleString;
import org.hornetq.api.core.client.ClientSessionFactoryImpl;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.cluster.BridgeConfiguration;
import org.hornetq.core.server.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.server.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.server.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.server.cluster.DivertConfiguration;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.utils.XMLConfigurationUtil;
import org.hornetq.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * ConfigurationImpl
 * This class allows the Configuration class to be configured via a config file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileConfiguration extends ConfigurationImpl
{
   private static final long serialVersionUID = -4766689627675039596L;

   private static final Logger log = Logger.getLogger(FileConfiguration.class);

   // Constants ------------------------------------------------------------------------

   private static final String DEFAULT_CONFIGURATION_URL = "hornetq-configuration.xml";

   private static final String CONFIGURATION_SCHEMA_URL = "schema/hornetq-configuration.xsd";

   // For a bridge confirmations must be activated or send acknowledgements won't return

   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = 1024 * 1024;

   // Static --------------------------------------------------------------------------

   // Attributes ----------------------------------------------------------------------

   private String configurationUrl = FileConfiguration.DEFAULT_CONFIGURATION_URL;

   private boolean started;

   // Public -------------------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      URL url = getClass().getClassLoader().getResource(configurationUrl);
      FileConfiguration.log.debug("Loading server configuration from " + url);

      Reader reader = new InputStreamReader(url.openStream());
      String xml = org.hornetq.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = org.hornetq.utils.XMLUtil.stringToElement(xml);
      org.hornetq.utils.XMLUtil.validate(e, FileConfiguration.CONFIGURATION_SCHEMA_URL);

      clustered = XMLConfigurationUtil.getBoolean(e, "clustered", clustered);

      backup = XMLConfigurationUtil.getBoolean(e, "backup", backup);

      sharedStore = XMLConfigurationUtil.getBoolean(e, "shared-store", sharedStore);

      // Defaults to true when using FileConfiguration
      fileDeploymentEnabled = XMLConfigurationUtil.getBoolean(e, "file-deployment-enabled", true);

      persistenceEnabled = XMLConfigurationUtil.getBoolean(e, "persistence-enabled", persistenceEnabled);

      persistDeliveryCountBeforeDelivery = XMLConfigurationUtil.getBoolean(e,
                                                                           "persist-delivery-count-before-delivery",
                                                                           persistDeliveryCountBeforeDelivery);

      // NOTE! All the defaults come from the super class

      scheduledThreadPoolMaxSize = XMLConfigurationUtil.getInteger(e,
                                                                   "scheduled-thread-pool-max-size",
                                                                   scheduledThreadPoolMaxSize,
                                                                   Validators.GT_ZERO);

      threadPoolMaxSize = XMLConfigurationUtil.getInteger(e,
                                                          "thread-pool-max-size",
                                                          threadPoolMaxSize,
                                                          Validators.MINUS_ONE_OR_GT_ZERO);

      securityEnabled = XMLConfigurationUtil.getBoolean(e, "security-enabled", securityEnabled);

      jmxManagementEnabled = XMLConfigurationUtil.getBoolean(e, "jmx-management-enabled", jmxManagementEnabled);

      jmxDomain = XMLConfigurationUtil.getString(e, "jmx-domain", jmxDomain, Validators.NOT_NULL_OR_EMPTY);

      securityInvalidationInterval = XMLConfigurationUtil.getLong(e,
                                                                  "security-invalidation-interval",
                                                                  securityInvalidationInterval,
                                                                  Validators.GT_ZERO);

      connectionTTLOverride = XMLConfigurationUtil.getLong(e,
                                                           "connection-ttl-override",
                                                           connectionTTLOverride,
                                                           Validators.MINUS_ONE_OR_GT_ZERO);

      asyncConnectionExecutionEnabled = XMLConfigurationUtil.getBoolean(e,
                                                                        "async-connection-execution-enabled",
                                                                        asyncConnectionExecutionEnabled);

      transactionTimeout = XMLConfigurationUtil.getLong(e,
                                                        "transaction-timeout",
                                                        transactionTimeout,
                                                        Validators.GT_ZERO);

      transactionTimeoutScanPeriod = XMLConfigurationUtil.getLong(e,
                                                                  "transaction-timeout-scan-period",
                                                                  transactionTimeoutScanPeriod,
                                                                  Validators.GT_ZERO);

      messageExpiryScanPeriod = XMLConfigurationUtil.getLong(e,
                                                             "message-expiry-scan-period",
                                                             messageExpiryScanPeriod,
                                                             Validators.GT_ZERO);

      messageExpiryThreadPriority = XMLConfigurationUtil.getInteger(e,
                                                                    "message-expiry-thread-priority",
                                                                    messageExpiryThreadPriority,
                                                                    Validators.THREAD_PRIORITY_RANGE);

      idCacheSize = XMLConfigurationUtil.getInteger(e, "id-cache-size", idCacheSize, Validators.GT_ZERO);

      persistIDCache = XMLConfigurationUtil.getBoolean(e, "persist-id-cache", persistIDCache);

      managementAddress = new SimpleString(XMLConfigurationUtil.getString(e,
                                                                          "management-address",
                                                                          managementAddress.toString(),
                                                                          Validators.NOT_NULL_OR_EMPTY));

      managementNotificationAddress = new SimpleString(XMLConfigurationUtil.getString(e,
                                                                                      "management-notification-address",
                                                                                      managementNotificationAddress.toString(),
                                                                                      Validators.NOT_NULL_OR_EMPTY));

      managementClusterPassword = XMLConfigurationUtil.getString(e,
                                                                 "management-cluster-password",
                                                                 managementClusterPassword,
                                                                 Validators.NOT_NULL_OR_EMPTY);

      managementClusterUser = XMLConfigurationUtil.getString(e,
                                                             "management-cluster-user",
                                                             managementClusterUser,
                                                             Validators.NOT_NULL_OR_EMPTY);

      logDelegateFactoryClassName = XMLConfigurationUtil.getString(e,
                                                                   "log-delegate-factory-class-name",
                                                                   logDelegateFactoryClassName,
                                                                   Validators.NOT_NULL_OR_EMPTY);

      NodeList interceptorNodes = e.getElementsByTagName("remoting-interceptors");

      ArrayList<String> interceptorList = new ArrayList<String>();

      if (interceptorNodes.getLength() > 0)
      {
         NodeList interceptors = interceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++)
         {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName()))
            {
               String clazz = interceptors.item(i).getTextContent();

               interceptorList.add(clazz);
            }
         }
      }

      interceptorClassNames = interceptorList;

      NodeList backups = e.getElementsByTagName("backup-connector-ref");

      // There should be only one - this will be enforced by the DTD

      if (backups.getLength() > 0)
      {
         Node backupNode = backups.item(0);

         backupConnectorName = backupNode.getAttributes().getNamedItem("connector-name").getNodeValue();
      }

      NodeList connectorNodes = e.getElementsByTagName("connector");

      for (int i = 0; i < connectorNodes.getLength(); i++)
      {
         Element connectorNode = (Element)connectorNodes.item(i);

         TransportConfiguration connectorConfig = parseTransportConfiguration(connectorNode);

         if (connectorConfig.getName() == null)
         {
            FileConfiguration.log.warn("Cannot deploy a connector with no name specified.");

            continue;
         }

         if (connectorConfigs.containsKey(connectorConfig.getName()))
         {
            FileConfiguration.log.warn("There is already a connector with name " + connectorConfig.getName() +
                                       " deployed. This one will not be deployed.");

            continue;
         }

         connectorConfigs.put(connectorConfig.getName(), connectorConfig);
      }

      NodeList acceptorNodes = e.getElementsByTagName("acceptor");

      for (int i = 0; i < acceptorNodes.getLength(); i++)
      {
         Element acceptorNode = (Element)acceptorNodes.item(i);

         TransportConfiguration acceptorConfig = parseTransportConfiguration(acceptorNode);

         acceptorConfigs.add(acceptorConfig);
      }

      NodeList bgNodes = e.getElementsByTagName("broadcast-group");

      for (int i = 0; i < bgNodes.getLength(); i++)
      {
         Element bgNode = (Element)bgNodes.item(i);

         parseBroadcastGroupConfiguration(bgNode);
      }

      NodeList dgNodes = e.getElementsByTagName("discovery-group");

      for (int i = 0; i < dgNodes.getLength(); i++)
      {
         Element dgNode = (Element)dgNodes.item(i);

         parseDiscoveryGroupConfiguration(dgNode);
      }

      NodeList brNodes = e.getElementsByTagName("bridge");

      for (int i = 0; i < brNodes.getLength(); i++)
      {
         Element mfNode = (Element)brNodes.item(i);

         parseBridgeConfiguration(mfNode);
      }

      NodeList gaNodes = e.getElementsByTagName("grouping-handler");

      for (int i = 0; i < gaNodes.getLength(); i++)
      {
         Element gaNode = (Element)gaNodes.item(i);

         parseGroupingHandlerConfiguration(gaNode);
      }

      NodeList ccNodes = e.getElementsByTagName("cluster-connection");

      for (int i = 0; i < ccNodes.getLength(); i++)
      {
         Element ccNode = (Element)ccNodes.item(i);

         parseClusterConnectionConfiguration(ccNode);
      }

      NodeList dvNodes = e.getElementsByTagName("divert");

      for (int i = 0; i < dvNodes.getLength(); i++)
      {
         Element dvNode = (Element)dvNodes.item(i);

         parseDivertConfiguration(dvNode);
      }

      // Persistence config

      largeMessagesDirectory = XMLConfigurationUtil.getString(e,
                                                              "large-messages-directory",
                                                              largeMessagesDirectory,
                                                              Validators.NOT_NULL_OR_EMPTY);

      bindingsDirectory = XMLConfigurationUtil.getString(e,
                                                         "bindings-directory",
                                                         bindingsDirectory,
                                                         Validators.NOT_NULL_OR_EMPTY);

      createBindingsDir = XMLConfigurationUtil.getBoolean(e, "create-bindings-dir", createBindingsDir);

      journalDirectory = XMLConfigurationUtil.getString(e,
                                                        "journal-directory",
                                                        journalDirectory,
                                                        Validators.NOT_NULL_OR_EMPTY);

      pagingDirectory = XMLConfigurationUtil.getString(e,
                                                       "paging-directory",
                                                       pagingDirectory,
                                                       Validators.NOT_NULL_OR_EMPTY);

      createJournalDir = XMLConfigurationUtil.getBoolean(e, "create-journal-dir", createJournalDir);

      String s = XMLConfigurationUtil.getString(e, "journal-type", journalType.toString(), Validators.JOURNAL_TYPE);

      if (s.equals(JournalType.NIO.toString()))
      {
         journalType = JournalType.NIO;
      }
      else if (s.equals(JournalType.ASYNCIO.toString()))
      {
         journalType = JournalType.ASYNCIO;
      }

      journalSyncTransactional = XMLConfigurationUtil.getBoolean(e,
                                                                 "journal-sync-transactional",
                                                                 journalSyncTransactional);

      journalSyncNonTransactional = XMLConfigurationUtil.getBoolean(e,
                                                                    "journal-sync-non-transactional",
                                                                    journalSyncNonTransactional);

      journalFileSize = XMLConfigurationUtil.getInteger(e, "journal-file-size", journalFileSize, Validators.GT_ZERO);

      int journalBufferTimeout = XMLConfigurationUtil.getInteger(e,
                                                                 "journal-buffer-timeout",
                                                                 journalType == JournalType.ASYNCIO ? ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO
                                                                                                   : ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO,
                                                                 Validators.GT_ZERO);

      int journalBufferSize = XMLConfigurationUtil.getInteger(e,
                                                              "journal-buffer-size",
                                                              journalType == JournalType.ASYNCIO ? ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE_AIO
                                                                                                : ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE_NIO,
                                                              Validators.GT_ZERO);

      int journalMaxIO = XMLConfigurationUtil.getInteger(e,
                                                         "journal-max-io",
                                                         journalType == JournalType.ASYNCIO ? ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_AIO
                                                                                           : ConfigurationImpl.DEFAULT_JOURNAL_MAX_IO_NIO,
                                                         Validators.GT_ZERO);

      if (journalType == JournalType.ASYNCIO)
      {
         journalBufferTimeout_AIO = journalBufferTimeout;
         journalBufferSize_AIO = journalBufferSize;
         journalMaxIO_AIO = journalMaxIO;
      }
      else
      {
         journalBufferTimeout_NIO = journalBufferTimeout;
         journalBufferSize_NIO = journalBufferSize;
         journalMaxIO_NIO = journalMaxIO;
      }

      journalMinFiles = XMLConfigurationUtil.getInteger(e, "journal-min-files", journalMinFiles, Validators.GT_ZERO);

      journalCompactMinFiles = XMLConfigurationUtil.getInteger(e,
                                                               "journal-compact-min-files",
                                                               journalCompactMinFiles,
                                                               Validators.GE_ZERO);

      journalCompactPercentage = XMLConfigurationUtil.getInteger(e,
                                                                 "journal-compact-percentage",
                                                                 journalCompactPercentage,
                                                                 Validators.PERCENTAGE);

      logJournalWriteRate = XMLConfigurationUtil.getBoolean(e,
                                                            "log-journal-write-rate",
                                                            ConfigurationImpl.DEFAULT_JOURNAL_LOG_WRITE_RATE);

      journalPerfBlastPages = XMLConfigurationUtil.getInteger(e,
                                                              "perf-blast-pages",
                                                              ConfigurationImpl.DEFAULT_JOURNAL_PERF_BLAST_PAGES,
                                                              Validators.MINUS_ONE_OR_GT_ZERO);

      runSyncSpeedTest = XMLConfigurationUtil.getBoolean(e, "run-sync-speed-test", runSyncSpeedTest);

      wildcardRoutingEnabled = XMLConfigurationUtil.getBoolean(e, "wild-card-routing-enabled", wildcardRoutingEnabled);

      messageCounterEnabled = XMLConfigurationUtil.getBoolean(e, "message-counter-enabled", messageCounterEnabled);

      messageCounterSamplePeriod = XMLConfigurationUtil.getLong(e,
                                                                "message-counter-sample-period",
                                                                messageCounterSamplePeriod,
                                                                Validators.GT_ZERO);

      messageCounterMaxDayHistory = XMLConfigurationUtil.getInteger(e,
                                                                    "message-counter-max-day-history",
                                                                    messageCounterMaxDayHistory,
                                                                    Validators.GT_ZERO);

      serverDumpInterval = XMLConfigurationUtil.getLong(e,
                                                        "server-dump-interval",
                                                        serverDumpInterval,
                                                        Validators.MINUS_ONE_OR_GT_ZERO); // in
      // milliseconds

      memoryWarningThreshold = XMLConfigurationUtil.getInteger(e,
                                                               "memory-warning-threshold",
                                                               memoryWarningThreshold,
                                                               Validators.PERCENTAGE);

      memoryMeasureInterval = XMLConfigurationUtil.getLong(e,
                                                           "memory-measure-interval",
                                                           memoryMeasureInterval,
                                                           Validators.MINUS_ONE_OR_GT_ZERO); // in

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      started = false;
   }

   public String getConfigurationUrl()
   {
      return configurationUrl;
   }

   public void setConfigurationUrl(final String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
   }

   // Private -------------------------------------------------------------------------

   private TransportConfiguration parseTransportConfiguration(final Element e)
   {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String clazz = XMLConfigurationUtil.getString(e, "factory-class", null, Validators.NOT_NULL_OR_EMPTY);

      Map<String, Object> params = new HashMap<String, Object>();

      NodeList paramsNodes = e.getElementsByTagName("param");

      for (int i = 0; i < paramsNodes.getLength(); i++)
      {
         Node paramNode = paramsNodes.item(i);
         NamedNodeMap attributes = paramNode.getAttributes();

         Node nkey = attributes.getNamedItem("key");

         String key = nkey.getTextContent();

         Node nValue = attributes.getNamedItem("value");

         params.put(key, nValue.getTextContent());
      }

      return new TransportConfiguration(clazz, params, name);
   }

   private void parseBroadcastGroupConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String localAddress = XMLConfigurationUtil.getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = XMLConfigurationUtil.getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = XMLConfigurationUtil.getString(e, "group-address", null, Validators.NOT_NULL_OR_EMPTY);

      int groupPort = XMLConfigurationUtil.getInteger(e, "group-port", -1, Validators.GT_ZERO);

      long broadcastPeriod = XMLConfigurationUtil.getLong(e,
                                                          "broadcast-period",
                                                          ConfigurationImpl.DEFAULT_BROADCAST_PERIOD,
                                                          Validators.GT_ZERO);

      NodeList children = e.getChildNodes();

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("connector-ref"))
         {
            String connectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();

            Node backupConnectorNode = child.getAttributes().getNamedItem("backup-connector-name");

            String backupConnectorName = null;

            if (backupConnectorNode != null)
            {
               backupConnectorName = backupConnectorNode.getNodeValue();
            }

            Pair<String, String> connectorInfo = new Pair<String, String>(connectorName, backupConnectorName);

            connectorNames.add(connectorInfo);
         }
      }

      BroadcastGroupConfiguration config = new BroadcastGroupConfiguration(name,
                                                                           localAddress,
                                                                           localBindPort,
                                                                           groupAddress,
                                                                           groupPort,
                                                                           broadcastPeriod,
                                                                           connectorNames);

      broadcastGroupConfigurations.add(config);
   }

   private void parseDiscoveryGroupConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String groupAddress = XMLConfigurationUtil.getString(e, "group-address", null, Validators.NOT_NULL_OR_EMPTY);

      int groupPort = XMLConfigurationUtil.getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      long refreshTimeout = XMLConfigurationUtil.getLong(e,
                                                         "refresh-timeout",
                                                         ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT,
                                                         Validators.GT_ZERO);

      DiscoveryGroupConfiguration config = new DiscoveryGroupConfiguration(name,
                                                                           groupAddress,
                                                                           groupPort,
                                                                           refreshTimeout);

      if (discoveryGroupConfigurations.containsKey(name))
      {
         FileConfiguration.log.warn("There is already a discovery group with name " + name +
                                    " deployed. This one will not be deployed.");

         return;
      }
      else
      {
         discoveryGroupConfigurations.put(name, config);
      }
   }

   private void parseClusterConnectionConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String address = XMLConfigurationUtil.getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      boolean duplicateDetection = XMLConfigurationUtil.getBoolean(e,
                                                                   "use-duplicate-detection",
                                                                   ConfigurationImpl.DEFAULT_CLUSTER_DUPLICATE_DETECTION);

      boolean forwardWhenNoConsumers = XMLConfigurationUtil.getBoolean(e,
                                                                       "forward-when-no-consumers",
                                                                       ConfigurationImpl.DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS);

      int maxHops = XMLConfigurationUtil.getInteger(e,
                                                    "max-hops",
                                                    ConfigurationImpl.DEFAULT_CLUSTER_MAX_HOPS,
                                                    Validators.GE_ZERO);

      long retryInterval = XMLConfigurationUtil.getLong(e,
                                                        "retry-interval",
                                                        ConfigurationImpl.DEFAULT_CLUSTER_RETRY_INTERVAL,
                                                        Validators.GT_ZERO);

      int confirmationWindowSize = XMLConfigurationUtil.getInteger(e,
                                                                   "confirmation-window-size",
                                                                   FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                   Validators.GT_ZERO);

      String discoveryGroupName = null;

      List<Pair<String, String>> connectorPairs = new ArrayList<Pair<String, String>>();

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("discovery-group-ref"))
         {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();
         }
         else if (child.getNodeName().equals("connector-ref"))
         {
            String connectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();

            Node backupNode = child.getAttributes().getNamedItem("backup-connector-name");

            String backupConnectorName = null;

            if (backupNode != null)
            {
               backupConnectorName = backupNode.getNodeValue();
            }

            Pair<String, String> connectorPair = new Pair<String, String>(connectorName, backupConnectorName);

            connectorPairs.add(connectorPair);
         }
      }

      ClusterConnectionConfiguration config;

      if (discoveryGroupName == null)
      {
         config = new ClusterConnectionConfiguration(name,
                                                     address,
                                                     retryInterval,
                                                     duplicateDetection,
                                                     forwardWhenNoConsumers,
                                                     maxHops,
                                                     confirmationWindowSize,
                                                     connectorPairs);
      }
      else
      {
         config = new ClusterConnectionConfiguration(name,
                                                     address,
                                                     retryInterval,
                                                     duplicateDetection,
                                                     forwardWhenNoConsumers,
                                                     maxHops,
                                                     confirmationWindowSize,
                                                     discoveryGroupName);
      }

      clusterConfigurations.add(config);
   }

   private void parseGroupingHandlerConfiguration(final Element node)
   {
      String name = node.getAttribute("name");
      String type = XMLConfigurationUtil.getString(node, "type", null, Validators.NOT_NULL_OR_EMPTY);
      String address = XMLConfigurationUtil.getString(node, "address", null, Validators.NOT_NULL_OR_EMPTY);
      Integer timeout = XMLConfigurationUtil.getInteger(node,
                                                        "timeout",
                                                        GroupingHandlerConfiguration.DEFAULT_TIMEOUT,
                                                        Validators.GT_ZERO);
      groupingHandlerConfiguration = new GroupingHandlerConfiguration(new SimpleString(name),
                                                                      type.equals(GroupingHandlerConfiguration.TYPE.LOCAL.getType()) ? GroupingHandlerConfiguration.TYPE.LOCAL
                                                                                                                                    : GroupingHandlerConfiguration.TYPE.REMOTE,
                                                                      new SimpleString(address),
                                                                      timeout);
   }

   private void parseBridgeConfiguration(final Element brNode)
   {
      String name = brNode.getAttribute("name");

      String queueName = XMLConfigurationUtil.getString(brNode, "queue-name", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = XMLConfigurationUtil.getString(brNode,
                                                                "forwarding-address",
                                                                null,
                                                                Validators.NOT_NULL_OR_EMPTY);

      String transformerClassName = XMLConfigurationUtil.getString(brNode,
                                                                   "transformer-class-name",
                                                                   null,
                                                                   Validators.NO_CHECK);

      long retryInterval = XMLConfigurationUtil.getLong(brNode,
                                                        "retry-interval",
                                                        HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                        Validators.GT_ZERO);

      // Default bridge conf
      int confirmationWindowSize = XMLConfigurationUtil.getInteger(brNode,
                                                                   "confirmation-window-size",
                                                                   FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                   Validators.GT_ZERO);

      double retryIntervalMultiplier = XMLConfigurationUtil.getDouble(brNode,
                                                                      "retry-interval-multiplier",
                                                                      HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                      Validators.GT_ZERO);

      int reconnectAttempts = XMLConfigurationUtil.getInteger(brNode,
                                                              "reconnect-attempts",
                                                              ConfigurationImpl.DEFAULT_BRIDGE_RECONNECT_ATTEMPTS,
                                                              Validators.MINUS_ONE_OR_GE_ZERO);

      boolean failoverOnServerShutdown = XMLConfigurationUtil.getBoolean(brNode,
                                                                         "failover-on-server-shutdown",
                                                                         HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);

      boolean useDuplicateDetection = XMLConfigurationUtil.getBoolean(brNode,
                                                                      "use-duplicate-detection",
                                                                      ConfigurationImpl.DEFAULT_BRIDGE_DUPLICATE_DETECTION);

      String filterString = null;

      Pair<String, String> connectorPair = null;

      String discoveryGroupName = null;

      NodeList children = brNode.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
         else if (child.getNodeName().equals("discovery-group-ref"))
         {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();
         }
         else if (child.getNodeName().equals("connector-ref"))
         {
            String connectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();

            Node backupNode = child.getAttributes().getNamedItem("backup-connector-name");

            String backupConnectorName = null;

            if (backupNode != null)
            {
               backupConnectorName = backupNode.getNodeValue();
            }

            connectorPair = new Pair<String, String>(connectorName, backupConnectorName);
         }
      }

      BridgeConfiguration config;

      if (connectorPair != null)
      {
         config = new BridgeConfiguration(name,
                                          queueName,
                                          forwardingAddress,
                                          filterString,
                                          transformerClassName,
                                          retryInterval,
                                          retryIntervalMultiplier,
                                          reconnectAttempts,
                                          failoverOnServerShutdown,
                                          useDuplicateDetection,
                                          confirmationWindowSize,
                                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                          connectorPair);
      }
      else
      {
         config = new BridgeConfiguration(name,
                                          queueName,
                                          forwardingAddress,
                                          filterString,
                                          transformerClassName,
                                          retryInterval,
                                          retryIntervalMultiplier,
                                          reconnectAttempts,
                                          failoverOnServerShutdown,
                                          useDuplicateDetection,
                                          confirmationWindowSize,
                                          HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                          discoveryGroupName);
      }

      bridgeConfigurations.add(config);
   }

   private void parseDivertConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String routingName = XMLConfigurationUtil.getString(e, "routing-name", null, Validators.NO_CHECK);

      String address = XMLConfigurationUtil.getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = XMLConfigurationUtil.getString(e,
                                                                "forwarding-address",
                                                                null,
                                                                Validators.NOT_NULL_OR_EMPTY);

      boolean exclusive = XMLConfigurationUtil.getBoolean(e, "exclusive", ConfigurationImpl.DEFAULT_DIVERT_EXCLUSIVE);

      String transformerClassName = XMLConfigurationUtil.getString(e,
                                                                   "transformer-class-name",
                                                                   null,
                                                                   Validators.NO_CHECK);

      String filterString = null;

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
      }

      DivertConfiguration config = new DivertConfiguration(name,
                                                           routingName,
                                                           address,
                                                           forwardingAddress,
                                                           exclusive,
                                                           filterString,
                                                           transformerClassName);

      divertConfigurations.add(config);
   }
}
