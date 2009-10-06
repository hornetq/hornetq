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

package org.hornetq.core.config.impl;

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.hornetq.core.config.impl.Validators.GE_ZERO;
import static org.hornetq.core.config.impl.Validators.GT_ZERO;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GE_ZERO;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GT_ZERO;
import static org.hornetq.core.config.impl.Validators.NOT_NULL_OR_EMPTY;
import static org.hornetq.core.config.impl.Validators.NO_CHECK;
import static org.hornetq.core.config.impl.Validators.PERCENTAGE;
import static org.hornetq.core.config.impl.Validators.THREAD_PRIORITY_RANGE;
import static org.hornetq.utils.XMLConfigurationUtil.getBoolean;
import static org.hornetq.utils.XMLConfigurationUtil.getDouble;
import static org.hornetq.utils.XMLConfigurationUtil.getInteger;
import static org.hornetq.utils.XMLConfigurationUtil.getLong;
import static org.hornetq.utils.XMLConfigurationUtil.getString;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.JournalType;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
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

   // Static --------------------------------------------------------------------------
   
   // Attributes ----------------------------------------------------------------------

   private String configurationUrl = DEFAULT_CONFIGURATION_URL;
   

   private boolean started;
   
   // Public -------------------------------------------------------------------------

   public synchronized void start() throws Exception
   {      
      if (started)
      {
         return;
      }
       
      URL url = getClass().getClassLoader().getResource(configurationUrl);
      log.debug("Loading server configuration from " + url);

      Reader reader = new InputStreamReader(url.openStream());
      String xml = org.hornetq.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = org.hornetq.utils.XMLUtil.stringToElement(xml);
      org.hornetq.utils.XMLUtil.validate(e, CONFIGURATION_SCHEMA_URL);

      clustered = getBoolean(e, "clustered", clustered);

      backup = getBoolean(e, "backup", backup);
      
      sharedStore = getBoolean(e, "shared-store", sharedStore);
      
      //Defaults to true when using FileConfiguration
      fileDeploymentEnabled = getBoolean(e, "file-deployment-enabled", true);
      
      persistenceEnabled = getBoolean(e, "persistence-enabled", persistenceEnabled);

      persistDeliveryCountBeforeDelivery = getBoolean(e, "persist-delivery-count-before-delivery", persistDeliveryCountBeforeDelivery);
      
      // NOTE! All the defaults come from the super class

      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-thread-pool-max-size", scheduledThreadPoolMaxSize, GT_ZERO);
      
      threadPoolMaxSize = getInteger(e, "thread-pool-max-size", threadPoolMaxSize, MINUS_ONE_OR_GT_ZERO);

      securityEnabled = getBoolean(e, "security-enabled", securityEnabled);

      jmxManagementEnabled = getBoolean(e, "jmx-management-enabled", jmxManagementEnabled);

      securityInvalidationInterval = getLong(e, "security-invalidation-interval", securityInvalidationInterval, GT_ZERO);

      connectionTTLOverride = getLong(e, "connection-ttl-override", connectionTTLOverride, MINUS_ONE_OR_GT_ZERO);
      
      asyncConnectionExecutionEnabled = getBoolean(e, "async-connection-execution-enabled", asyncConnectionExecutionEnabled);

      transactionTimeout = getLong(e, "transaction-timeout", transactionTimeout, GT_ZERO);

      transactionTimeoutScanPeriod = getLong(e, "transaction-timeout-scan-period", transactionTimeoutScanPeriod, GT_ZERO);

      messageExpiryScanPeriod = getLong(e, "message-expiry-scan-period", messageExpiryScanPeriod, GT_ZERO);

      messageExpiryThreadPriority = getInteger(e, "message-expiry-thread-priority", messageExpiryThreadPriority, THREAD_PRIORITY_RANGE);

      idCacheSize = getInteger(e, "id-cache-size", idCacheSize, GT_ZERO);

      persistIDCache = getBoolean(e, "persist-id-cache", persistIDCache);

      managementAddress = new SimpleString(getString(e, "management-address", managementAddress.toString(), NOT_NULL_OR_EMPTY));

      managementNotificationAddress = new SimpleString(getString(e,
                                                                 "management-notification-address",
                                                                 managementNotificationAddress.toString(), NOT_NULL_OR_EMPTY));

      managementClusterPassword = getString(e, "management-cluster-password", managementClusterPassword, NOT_NULL_OR_EMPTY);

      managementClusterUser = getString(e, "management-cluster-user", managementClusterUser, NOT_NULL_OR_EMPTY);

      managementRequestTimeout = getLong(e, "management-request-timeout", managementRequestTimeout, GT_ZERO);
      
      logDelegateFactoryClassName = getString(e, "log-delegate-factory-class-name", logDelegateFactoryClassName, NOT_NULL_OR_EMPTY);

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
            log.warn("Cannot deploy a connector with no name specified.");

            continue;
         }

         if (connectorConfigs.containsKey(connectorConfig.getName()))
         {
            log.warn("There is already a connector with name " + connectorConfig.getName() +
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

      largeMessagesDirectory = getString(e, "large-messages-directory", largeMessagesDirectory, NOT_NULL_OR_EMPTY);

      bindingsDirectory = getString(e, "bindings-directory", bindingsDirectory, NOT_NULL_OR_EMPTY);

      createBindingsDir = getBoolean(e, "create-bindings-dir", createBindingsDir);

      journalDirectory = getString(e, "journal-directory", journalDirectory, NOT_NULL_OR_EMPTY);

      pagingDirectory = getString(e, "paging-directory", pagingDirectory, NOT_NULL_OR_EMPTY);

      createJournalDir = getBoolean(e, "create-journal-dir", createJournalDir);

      String s = getString(e, "journal-type", journalType.toString(), Validators.JOURNAL_TYPE);

      if (s.equals(JournalType.NIO.toString()))
      {
         journalType = JournalType.NIO;
      }
      else if (s.equals(JournalType.ASYNCIO.toString()))
      {
         journalType = JournalType.ASYNCIO;
      }

      journalSyncTransactional = getBoolean(e, "journal-sync-transactional", journalSyncTransactional);

      journalSyncNonTransactional = getBoolean(e, "journal-sync-non-transactional", journalSyncNonTransactional);

      journalFileSize = getInteger(e, "journal-file-size", journalFileSize, GT_ZERO);
      
      journalAIOFlushSync = getBoolean(e, "journal-aio-flush-on-sync", DEFAULT_JOURNAL_AIO_FLUSH_SYNC);
      
      journalAIOBufferTimeout = getInteger(e, "journal-aio-buffer-timeout", DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT, GT_ZERO);
      
      journalAIOBufferSize = getInteger(e, "journal-aio-buffer-size", DEFAULT_JOURNAL_AIO_BUFFER_SIZE, GT_ZERO);

      journalMinFiles = getInteger(e, "journal-min-files", journalMinFiles, GT_ZERO);
      
      journalCompactMinFiles = getInteger(e, "journal-compact-min-files", journalCompactMinFiles, GE_ZERO);

      journalCompactPercentage = getInteger(e, "journal-compact-percentage", journalCompactPercentage, PERCENTAGE);

      journalMaxAIO = getInteger(e, "journal-max-aio", journalMaxAIO, GT_ZERO);
      
      logJournalWriteRate = getBoolean(e, "log-journal-write-rate", DEFAULT_JOURNAL_LOG_WRITE_RATE);
      
      journalPerfBlastPages = getInteger(e, "perf-blast-pages", DEFAULT_JOURNAL_PERF_BLAST_PAGES, MINUS_ONE_OR_GT_ZERO);

      wildcardRoutingEnabled = getBoolean(e, "wild-card-routing-enabled", wildcardRoutingEnabled);

      messageCounterEnabled = getBoolean(e, "message-counter-enabled", messageCounterEnabled);

      messageCounterSamplePeriod = getLong(e, "message-counter-sample-period", messageCounterSamplePeriod, GT_ZERO);

      messageCounterMaxDayHistory = getInteger(e, "message-counter-max-day-history", messageCounterMaxDayHistory, GT_ZERO);
      
      serverDumpInterval = getLong(e, "server-dump-interval", serverDumpInterval, MINUS_ONE_OR_GT_ZERO); // in milliseconds

      memoryWarningThreshold = getInteger(e, "memory-warning-threshold", memoryWarningThreshold, PERCENTAGE);
      
      memoryMeasureInterval = getLong(e, "memory-measure-interval", memoryMeasureInterval, GT_ZERO); // in milliseconds
      
      started = true;
   }
   
   public synchronized void stop() throws Exception
   {
      super.stop();
      
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

      String clazz = getString(e, "factory-class", null, NOT_NULL_OR_EMPTY);

      Map<String, Object> params = new HashMap<String, Object>();

      NodeList paramsNodes = e.getElementsByTagName("param");

      for (int i = 0; i < paramsNodes.getLength(); i++)
      {
         Node paramNode = paramsNodes.item(i);
         NamedNodeMap attributes =paramNode.getAttributes();

         Node nkey = attributes.getNamedItem("key");

         String key = nkey.getTextContent();

         Node nValue = attributes.getNamedItem("value");

         Node nType = attributes.getNamedItem("type");

         String type = nType.getTextContent();

         if (type.equalsIgnoreCase("Integer"))
         {
            int iVal = org.hornetq.utils.XMLUtil.parseInt(nValue);

            params.put(key, iVal);
         }
         else if (type.equalsIgnoreCase("Long"))
         {
            long lVal = org.hornetq.utils.XMLUtil.parseLong(nValue);

            params.put(key, lVal);
         }
         else if (type.equalsIgnoreCase("String"))
         {
            params.put(key, nValue.getTextContent().trim());
         }
         else if (type.equalsIgnoreCase("Boolean"))
         {
            boolean bVal = org.hornetq.utils.XMLUtil.parseBoolean(nValue);

            params.put(key, bVal);
         }
         else
         {
            throw new IllegalArgumentException("Invalid parameter type " + type);
         }
      }

      return new TransportConfiguration(clazz, params, name);
   }

   private void parseBroadcastGroupConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String localAddress = getString(e, "local-bind-address", null, NO_CHECK);
      
      int localBindPort = getInteger(e, "local-bind-port", -1, MINUS_ONE_OR_GT_ZERO);

      String groupAddress = getString(e, "group-address", null, NOT_NULL_OR_EMPTY);

      int groupPort = getInteger(e, "group-port", -1, GT_ZERO);

      long broadcastPeriod = getLong(e, "broadcast-period", DEFAULT_BROADCAST_PERIOD, GT_ZERO);

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

      String groupAddress = getString(e, "group-address", null, NOT_NULL_OR_EMPTY);

      int groupPort = getInteger(e, "group-port", -1, MINUS_ONE_OR_GT_ZERO);

      long refreshTimeout = getLong(e, "refresh-timeout", DEFAULT_BROADCAST_REFRESH_TIMEOUT, GT_ZERO);

      DiscoveryGroupConfiguration config = new DiscoveryGroupConfiguration(name,
                                                                           groupAddress,
                                                                           groupPort,
                                                                           refreshTimeout);

      if (discoveryGroupConfigurations.containsKey(name))
      {
         log.warn("There is already a discovery group with name " + name + " deployed. This one will not be deployed.");

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

      String address = getString(e, "address", null, NOT_NULL_OR_EMPTY);

      boolean duplicateDetection = getBoolean(e, "use-duplicate-detection", DEFAULT_CLUSTER_DUPLICATE_DETECTION);

      boolean forwardWhenNoConsumers = getBoolean(e, "forward-when-no-consumers", DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS);

      int maxHops = getInteger(e, "max-hops", DEFAULT_CLUSTER_MAX_HOPS, GE_ZERO);

      long retryInterval = getLong(e, "retry-interval", (long)DEFAULT_CLUSTER_RETRY_INTERVAL, GT_ZERO);

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
                                                     discoveryGroupName);
      }

      clusterConfigurations.add(config);
   }

   private void parseBridgeConfiguration(final Element brNode)
   {
      String name = brNode.getAttribute("name");

      String queueName = getString(brNode, "queue-name", null, NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(brNode, "forwarding-address", null, NOT_NULL_OR_EMPTY);

      String transformerClassName = getString(brNode, "transformer-class-name", null, NO_CHECK);

      long retryInterval = getLong(brNode, "retry-interval", DEFAULT_RETRY_INTERVAL, GT_ZERO);

      double retryIntervalMultiplier = getDouble(brNode, "retry-interval-multiplier", DEFAULT_RETRY_INTERVAL_MULTIPLIER, GT_ZERO);
      
      int reconnectAttempts = getInteger(brNode, "reconnect-attempts", DEFAULT_BRIDGE_RECONNECT_ATTEMPTS, MINUS_ONE_OR_GE_ZERO);

      boolean failoverOnServerShutdown = getBoolean(brNode, "failover-on-server-shutdown", ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);

      boolean useDuplicateDetection = getBoolean(brNode, "use-duplicate-detection", DEFAULT_BRIDGE_DUPLICATE_DETECTION);

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
                                          discoveryGroupName);
      }

      bridgeConfigurations.add(config);
   }

   private void parseDivertConfiguration(final Element e)
   {
      String name = e.getAttribute("name");

      String routingName = getString(e, "routing-name", null, NO_CHECK);

      String address = getString(e, "address", null, NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(e, "forwarding-address", null, NOT_NULL_OR_EMPTY);

      boolean exclusive = getBoolean(e, "exclusive", DEFAULT_DIVERT_EXCLUSIVE);

      String transformerClassName = getString(e, "transformer-class-name", null, NO_CHECK);

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
