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

package org.jboss.messaging.core.config.impl;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.XMLUtil;
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

   private static final String DEFAULT_CONFIGURATION_URL = "jbm-configuration.xml";

   private static final String CONFIGURATION_SCHEMA_URL = "jbm-configuration.xsd";

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
      Reader reader = new InputStreamReader(url.openStream());
      String xml = org.jboss.messaging.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      org.jboss.messaging.utils.XMLUtil.validate(e, CONFIGURATION_SCHEMA_URL);

      clustered = getBoolean(e, "clustered", clustered);

      backup = getBoolean(e, "backup", backup);

      persistDeliveryCountBeforeDelivery = getBoolean(e, "persist-delivery-count-before-delivery", persistDeliveryCountBeforeDelivery);
      
      queueActivationTimeout = getLong(e, "queue-activation-timeout", queueActivationTimeout);

      // NOTE! All the defaults come from the super class

      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-max-pool-size", scheduledThreadPoolMaxSize);

      securityEnabled = getBoolean(e, "security-enabled", securityEnabled);

      jmxManagementEnabled = getBoolean(e, "jmx-management-enabled", jmxManagementEnabled);

      securityInvalidationInterval = getLong(e, "security-invalidation-interval", securityInvalidationInterval);

      connectionScanPeriod = getLong(e, "connection-scan-period", connectionScanPeriod);

      connectionTTLOverride = getLong(e, "connection-ttl-override", connectionTTLOverride);

      transactionTimeout = getLong(e, "transaction-timeout", transactionTimeout);

      transactionTimeoutScanPeriod = getLong(e, "transaction-timeout-scan-period", transactionTimeoutScanPeriod);

      messageExpiryScanPeriod = getLong(e, "message-expiry-scan-period", messageExpiryScanPeriod);

      messageExpiryThreadPriority = getInteger(e, "message-expiry-thread-priority", messageExpiryThreadPriority);

      idCacheSize = getInteger(e, "id-cache-size", idCacheSize);

      persistIDCache = getBoolean(e, "persist-id-cache", persistIDCache);

      managementAddress = new SimpleString(getString(e, "management-address", managementAddress.toString()));

      managementNotificationAddress = new SimpleString(getString(e,
                                                                 "management-notification-address",
                                                                 managementNotificationAddress.toString()));

      managementClusterPassword = getString(e, "management-cluster-password", managementClusterPassword.toString());

      managementRequestTimeout = getLong(e, "management-request-timeout", managementRequestTimeout);

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
         Node connectorNode = connectorNodes.item(i);

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
         Node acceptorNode = acceptorNodes.item(i);

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

      largeMessagesDirectory = getString(e, "large-messages-directory", largeMessagesDirectory);

      bindingsDirectory = getString(e, "bindings-directory", bindingsDirectory);

      createBindingsDir = getBoolean(e, "create-bindings-dir", createBindingsDir);

      journalDirectory = getString(e, "journal-directory", journalDirectory);

      pagingMaxThreads = getInteger(e, "paging-max-threads", pagingMaxThreads);

      pagingDirectory = getString(e, "paging-directory", pagingDirectory);

      pagingMaxGlobalSize = getLong(e, "paging-max-global-size-bytes", pagingMaxGlobalSize);

      pageWatermarkSize = getInteger(e, "paging-global-watermark-size", pageWatermarkSize);
      
      createJournalDir = getBoolean(e, "create-journal-dir", createJournalDir);

      String s = getString(e, "journal-type", journalType.toString());

      if (s == null || !s.equals(JournalType.NIO.toString()) && !s.equals(JournalType.ASYNCIO.toString()))
      {
         throw new IllegalArgumentException("Invalid journal type " + s);
      }

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

      journalFileSize = getInteger(e, "journal-file-size", journalFileSize);

      journalBufferReuseSize = getInteger(e, "journal-buffer-reuse-size", journalBufferReuseSize);

      journalMinFiles = getInteger(e, "journal-min-files", journalMinFiles);

      journalMaxAIO = getInteger(e, "journal-max-aio", journalMaxAIO);

      wildcardRoutingEnabled = getBoolean(e, "wild-card-routing-enabled", wildcardRoutingEnabled);

      messageCounterEnabled = getBoolean(e, "message-counter-enabled", messageCounterEnabled);
      
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

   private Boolean getBoolean(final Element e, final String name, final Boolean def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return org.jboss.messaging.utils.XMLUtil.parseBoolean(nl.item(0));
      }
      return def;
   }

   private Integer getInteger(final Element e, final String name, final Integer def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return XMLUtil.parseInt(nl.item(0));
      }
      return def;
   }

   private Long getLong(final Element e, final String name, final Long def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return org.jboss.messaging.utils.XMLUtil.parseLong(nl.item(0));
      }
      return def;
   }

   private String getString(final Element e, final String name, final String def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return nl.item(0).getTextContent().trim();
      }
      return def;
   }

   private TransportConfiguration parseTransportConfiguration(final Node node)
   {
      Node nameNode = node.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      NodeList children = node.getChildNodes();

      String clazz = null;

      Map<String, Object> params = new HashMap<String, Object>();

      for (int i = 0; i < children.getLength(); i++)
      {
         String nodeName = children.item(i).getNodeName();

         if ("factory-class".equalsIgnoreCase(nodeName))
         {
            clazz = children.item(i).getTextContent();
         }
         else if ("param".equalsIgnoreCase(nodeName))
         {
            NamedNodeMap attributes = children.item(i).getAttributes();

            Node nkey = attributes.getNamedItem("key");

            String key = nkey.getTextContent();

            Node nValue = attributes.getNamedItem("value");

            Node nType = attributes.getNamedItem("type");

            String type = nType.getTextContent();

            if (type.equalsIgnoreCase("Integer"))
            {
               int iVal = org.jboss.messaging.utils.XMLUtil.parseInt(nValue);

               params.put(key, iVal);
            }
            else if (type.equalsIgnoreCase("Long"))
            {
               long lVal = org.jboss.messaging.utils.XMLUtil.parseLong(nValue);

               params.put(key, lVal);
            }
            else if (type.equalsIgnoreCase("String"))
            {
               params.put(key, nValue.getTextContent().trim());
            }
            else if (type.equalsIgnoreCase("Boolean"))
            {
               boolean bVal = org.jboss.messaging.utils.XMLUtil.parseBoolean(nValue);

               params.put(key, bVal);
            }
            else
            {
               throw new IllegalArgumentException("Invalid parameter type " + type);
            }
         }
      }

      return new TransportConfiguration(clazz, params, name);
   }

   private void parseBroadcastGroupConfiguration(final Element bgNode)
   {
      String name = bgNode.getAttribute("name");

      int localBindPort = -1;

      String groupAddress = null;

      int groupPort = -1;

      long broadcastPeriod = ConfigurationImpl.DEFAULT_BROADCAST_PERIOD;

      NodeList children = bgNode.getChildNodes();

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("local-bind-port"))
         {
            localBindPort = org.jboss.messaging.utils.XMLUtil.parseInt(child);
         }
         else if (child.getNodeName().equals("group-address"))
         {
            groupAddress = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("group-port"))
         {
            groupPort = org.jboss.messaging.utils.XMLUtil.parseInt(child);
         }
         else if (child.getNodeName().equals("broadcast-period"))
         {
            broadcastPeriod = org.jboss.messaging.utils.XMLUtil.parseLong(child);
         }
         else if (child.getNodeName().equals("connector-ref"))
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
                                                                           localBindPort,
                                                                           groupAddress,
                                                                           groupPort,
                                                                           broadcastPeriod,
                                                                           connectorNames);

      broadcastGroupConfigurations.add(config);
   }

   private void parseDiscoveryGroupConfiguration(final Element bgNode)
   {
      String name = bgNode.getAttribute("name");

      String groupAddress = null;

      int groupPort = -1;

      long refreshTimeout = ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT;

      NodeList children = bgNode.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("group-address"))
         {
            groupAddress = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("group-port"))
         {
            groupPort = org.jboss.messaging.utils.XMLUtil.parseInt(child);
         }
         else if (child.getNodeName().equals("refresh-timeout"))
         {
            refreshTimeout = org.jboss.messaging.utils.XMLUtil.parseLong(child);
         }
      }

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

   private void parseClusterConnectionConfiguration(final Element brNode)
   {
      String name = null;

      String address = null;

      boolean duplicateDetection = DEFAULT_CLUSTER_DUPLICATE_DETECTION;

      boolean forwardWhenNoConsumers = DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS;

      String discoveryGroupName = null;

      int maxHops = DEFAULT_CLUSTER_MAX_HOPS;

      long retryInterval = DEFAULT_RETRY_INTERVAL;

      List<Pair<String, String>> connectorPairs = new ArrayList<Pair<String, String>>();

      name = brNode.getAttribute("name");

      NodeList children = brNode.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("address"))
         {
            address = child.getTextContent().trim();
         }  
         else if (child.getNodeName().equals("retry-interval"))
         {
            retryInterval = XMLUtil.parseLong(child);
         }        
         else if (child.getNodeName().equals("use-duplicate-detection"))
         {
            duplicateDetection = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
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
      String name = null;

      String queueName = null;

      String forwardingAddress = null;

      String filterString = null;

      Pair<String, String> connectorPair = null;

      String discoveryGroupName = null;

      String transformerClassName = null;

      long retryInterval = DEFAULT_RETRY_INTERVAL;

      double retryIntervalMultiplier = DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      int reconnectAttempts = ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;

      boolean failoverOnServerShutdown = ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

      boolean useDuplicateDetection = DEFAULT_BRIDGE_DUPLICATE_DETECTION;

      name = brNode.getAttribute("name");

      NodeList children = brNode.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("queue-name"))
         {
            queueName = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("forwarding-address"))
         {
            forwardingAddress = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
         else if (child.getNodeName().equals("transformer-class-name"))
         {
            transformerClassName = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("retry-interval"))
         {
            retryInterval = org.jboss.messaging.utils.XMLUtil.parseLong(child);
         }
         else if (child.getNodeName().equals("retry-interval-multiplier"))
         {
            retryIntervalMultiplier = XMLUtil.parseDouble(child);
         }
         else if (child.getNodeName().equals("reconnect-attempts"))
         {
            reconnectAttempts = org.jboss.messaging.utils.XMLUtil.parseInt(child);
         }
         else if (child.getNodeName().equals("failover-on-server-shutdown"))
         {
            failoverOnServerShutdown = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
         }
         else if (child.getNodeName().equals("use-duplicate-detection"))
         {
            useDuplicateDetection = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
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

   private void parseDivertConfiguration(final Element dvNode)
   {
      String name = null;

      String routingName = null;

      String address = null;

      String forwardingAddress = null;

      boolean exclusive = DEFAULT_DIVERT_EXCLUSIVE;

      String filterString = null;

      String transformerClassName = null;

      NodeList children = dvNode.getChildNodes();

      name = dvNode.getAttribute("name");
      
      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("routing-name"))
         {
            routingName = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("address"))
         {
            address = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("forwarding-address"))
         {
            forwardingAddress = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("exclusive"))
         {
            exclusive = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
         }
         else if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
         else if (child.getNodeName().equals("transformer-class-name"))
         {
            transformerClassName = child.getTextContent().trim();
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
