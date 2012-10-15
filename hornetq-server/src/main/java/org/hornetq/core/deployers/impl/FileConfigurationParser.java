/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.deployers.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.BroadcastEndpointFactoryConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.JGroupsBroadcastGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConnectorServiceConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.config.impl.Validators;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalConstants;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.DefaultSensitiveStringCodec;
import org.hornetq.utils.Pair;
import org.hornetq.utils.PasswordMaskingUtil;
import org.hornetq.utils.SensitiveDataCodec;
import org.hornetq.utils.XMLConfigurationUtil;
import org.hornetq.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class will parse the XML associated with the File Configuration XSD
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public final class FileConfigurationParser
{

   // Constants -----------------------------------------------------

   private static final String CONFIGURATION_SCHEMA_URL = "schema/hornetq-configuration.xsd";

   // Security Parsing
   public static final String SECURITY_ELEMENT_NAME = "security-setting";

   private static final String PERMISSION_ELEMENT_NAME = "permission";

   private static final String TYPE_ATTR_NAME = "type";

   private static final String ROLES_ATTR_NAME = "roles";

   static final String CREATEDURABLEQUEUE_NAME = "createDurableQueue";

   private static final String DELETEDURABLEQUEUE_NAME = "deleteDurableQueue";

   private static final String CREATE_NON_DURABLE_QUEUE_NAME = "createNonDurableQueue";

   private static final String DELETE_NON_DURABLE_QUEUE_NAME = "deleteNonDurableQueue";

   // HORNETQ-309 we keep supporting these attribute names for compatibility
   private static final String CREATETEMPQUEUE_NAME = "createTempQueue";

   private static final String DELETETEMPQUEUE_NAME = "deleteTempQueue";

   private static final String SEND_NAME = "send";

   private static final String CONSUME_NAME = "consume";

   private static final String MANAGE_NAME = "manage";

   // Address parsing

   private static final String DEAD_LETTER_ADDRESS_NODE_NAME = "dead-letter-address";

   private static final String EXPIRY_ADDRESS_NODE_NAME = "expiry-address";

   private static final String EXPIRY_DELAY_NODE_NAME = "expiry-delay";

   private static final String REDELIVERY_DELAY_NODE_NAME = "redelivery-delay";

   private static final String REDELIVERY_DELAY_MULTIPLIER_NODE_NAME = "redelivery-delay-multiplier";

   private static final String MAX_REDELIVERY_DELAY_NODE_NAME = "max-redelivery-delay";

   private static final String MAX_DELIVERY_ATTEMPTS = "max-delivery-attempts";

   private static final String MAX_SIZE_BYTES_NODE_NAME = "max-size-bytes";

   private static final String ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME = "address-full-policy";

   private static final String PAGE_SIZE_BYTES_NODE_NAME = "page-size-bytes";

   private static final String PAGE_MAX_CACHE_SIZE_NODE_NAME = "page-max-cache-size";

   private static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME = "message-counter-history-day-limit";

   private static final String LVQ_NODE_NAME = "last-value-queue";

   private static final String REDISTRIBUTION_DELAY_NODE_NAME = "redistribution-delay";

   private static final String SEND_TO_DLA_ON_NO_ROUTE = "send-to-dla-on-no-route";

   // Attributes ----------------------------------------------------

   private boolean validateAIO = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * @return the validateAIO
    */
   public boolean isValidateAIO()
   {
      return validateAIO;
   }

   /**
    * @param validateAIO the validateAIO to set
    */
   public void setValidateAIO(final boolean validateAIO)
   {
      this.validateAIO = validateAIO;
   }

   public Configuration parseMainConfig(final InputStream input) throws Exception
   {

      Reader reader = new InputStreamReader(input);
      String xml = org.hornetq.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = org.hornetq.utils.XMLUtil.stringToElement(xml);

      Configuration config = new ConfigurationImpl();

      parseMainConfig(e, config);

      return config;
   }

   public void parseMainConfig(final Element e, final Configuration config) throws Exception
   {
      XMLUtil.validate(e, FileConfigurationParser.CONFIGURATION_SCHEMA_URL);

      config.setName(XMLConfigurationUtil.getString(e, "name", config.getName(), Validators.NO_CHECK));

      NodeList elems = e.getElementsByTagName("clustered");
      if (elems != null && elems.getLength() > -1)
      {
         HornetQServerLogger.LOGGER.deprecatedConfigurationOption("clustered");

      }

      config.setCheckForLiveServer(XMLConfigurationUtil.getBoolean(e, "check-for-live-server", config.isClustered()));

      config.setAllowAutoFailBack(XMLConfigurationUtil.getBoolean(e, "allow-failback", config.isClustered()));

      config.setBackupGroupName(XMLConfigurationUtil.getString(e, "backup-group-name", config.getBackupGroupName(),
                                                               Validators.NO_CHECK));

      config.setFailbackDelay(XMLConfigurationUtil.getLong(e, "failback-delay", config.getFailbackDelay(), Validators.GT_ZERO));

      config.setFailoverOnServerShutdown(XMLConfigurationUtil.getBoolean(e,
                                                                         "failover-on-shutdown",
                                                                         config.isFailoverOnServerShutdown()));
      config.setReplicationClustername(XMLConfigurationUtil.getString(e, "replication-clustername", null,
                                                                      Validators.NO_CHECK));

      config.setBackup(XMLConfigurationUtil.getBoolean(e, "backup", config.isBackup()));

      config.setSharedStore(XMLConfigurationUtil.getBoolean(e, "shared-store", config.isSharedStore()));

      // Defaults to true when using FileConfiguration
      config.setFileDeploymentEnabled(XMLConfigurationUtil.getBoolean(e,
                                                                      "file-deployment-enabled",
                                                                      config instanceof FileConfiguration));

      config.setPersistenceEnabled(XMLConfigurationUtil.getBoolean(e,
                                                                   "persistence-enabled",
                                                                   config.isPersistenceEnabled()));

      config.setPersistDeliveryCountBeforeDelivery(XMLConfigurationUtil.getBoolean(e,
                                                                                   "persist-delivery-count-before-delivery",
                                                                                   config.isPersistDeliveryCountBeforeDelivery()));

      config.setScheduledThreadPoolMaxSize(XMLConfigurationUtil.getInteger(e,
                                                                           "scheduled-thread-pool-max-size",
                                                                           config.getScheduledThreadPoolMaxSize(),
                                                                           Validators.GT_ZERO));

      config.setThreadPoolMaxSize(XMLConfigurationUtil.getInteger(e,
                                                                  "thread-pool-max-size",
                                                                  config.getThreadPoolMaxSize(),
                                                                  Validators.MINUS_ONE_OR_GT_ZERO));

      config.setSecurityEnabled(XMLConfigurationUtil.getBoolean(e, "security-enabled", config.isSecurityEnabled()));

      config.setJMXManagementEnabled(XMLConfigurationUtil.getBoolean(e,
                                                                     "jmx-management-enabled",
                                                                     config.isJMXManagementEnabled()));

      config.setJMXDomain(XMLConfigurationUtil.getString(e,
                                                         "jmx-domain",
                                                         config.getJMXDomain(),
                                                         Validators.NOT_NULL_OR_EMPTY));

      config.setSecurityInvalidationInterval(XMLConfigurationUtil.getLong(e,
                                                                          "security-invalidation-interval",
                                                                          config.getSecurityInvalidationInterval(),
                                                                          Validators.GT_ZERO));

      config.setConnectionTTLOverride(XMLConfigurationUtil.getLong(e,
                                                                   "connection-ttl-override",
                                                                   config.getConnectionTTLOverride(),
                                                                   Validators.MINUS_ONE_OR_GT_ZERO));

      config.setEnabledAsyncConnectionExecution(XMLConfigurationUtil.getBoolean(e,
                                                                                "async-connection-execution-enabled",
                                                                                config.isAsyncConnectionExecutionEnabled()));

      config.setTransactionTimeout(XMLConfigurationUtil.getLong(e,
                                                                "transaction-timeout",
                                                                config.getTransactionTimeout(),
                                                                Validators.GT_ZERO));

      config.setTransactionTimeoutScanPeriod(XMLConfigurationUtil.getLong(e,
                                                                          "transaction-timeout-scan-period",
                                                                          config.getTransactionTimeoutScanPeriod(),
                                                                          Validators.GT_ZERO));

      config.setMessageExpiryScanPeriod(XMLConfigurationUtil.getLong(e,
                                                                     "message-expiry-scan-period",
                                                                     config.getMessageExpiryScanPeriod(),
                                                                     Validators.MINUS_ONE_OR_GT_ZERO));

      config.setMessageExpiryThreadPriority(XMLConfigurationUtil.getInteger(e,
                                                                            "message-expiry-thread-priority",
                                                                            config.getMessageExpiryThreadPriority(),
                                                                            Validators.THREAD_PRIORITY_RANGE));

      config.setIDCacheSize(XMLConfigurationUtil.getInteger(e,
                                                            "id-cache-size",
                                                            config.getIDCacheSize(),
                                                            Validators.GT_ZERO));

      config.setPersistIDCache(XMLConfigurationUtil.getBoolean(e, "persist-id-cache", config.isPersistIDCache()));

      config.setManagementAddress(new SimpleString(XMLConfigurationUtil.getString(e,
                                                                                  "management-address",
                                                                                  config.getManagementAddress()
                                                                                        .toString(),
                                                                                  Validators.NOT_NULL_OR_EMPTY)));

      config.setManagementNotificationAddress(new SimpleString(XMLConfigurationUtil.getString(e,
                                                                                              "management-notification-address",
                                                                                              config.getManagementNotificationAddress()
                                                                                                    .toString(),
                                                                                              Validators.NOT_NULL_OR_EMPTY)));

      config.setMaskPassword(XMLConfigurationUtil.getBoolean(e, "mask-password", false));

      config.setPasswordCodec(XMLConfigurationUtil.getString(e, "password-codec", DefaultSensitiveStringCodec.class.getName(),
                                                                   Validators.NOT_NULL_OR_EMPTY));

      // parsing cluster password
      String passwordText = XMLConfigurationUtil.getString(e, "cluster-password", null, Validators.NO_CHECK);

      final boolean maskText = config.isMaskPassword();

      if (passwordText != null)
      {
         if (maskText)
         {
            SensitiveDataCodec<String> codec = PasswordMaskingUtil.getCodec(config.getPasswordCodec());
            config.setClusterPassword(codec.decode(passwordText));
         }
         else
         {
            config.setClusterPassword(passwordText);
         }
      }

      config.setClusterUser(XMLConfigurationUtil.getString(e,
                                                           "cluster-user",
                                                           config.getClusterUser(),
                                                           Validators.NO_CHECK));

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

      config.setInterceptorClassNames(interceptorList);

      NodeList connectorNodes = e.getElementsByTagName("connector");

      for (int i = 0; i < connectorNodes.getLength(); i++)
      {
         Element connectorNode = (Element)connectorNodes.item(i);

         TransportConfiguration connectorConfig = parseTransportConfiguration(connectorNode, config);

         if (connectorConfig.getName() == null)
         {
            HornetQServerLogger.LOGGER.connectorWithNoName();

            continue;
         }

         if (config.getConnectorConfigurations().containsKey(connectorConfig.getName()))
         {
            HornetQServerLogger.LOGGER.connectorAlreadyDeployed(connectorConfig.getName());

            continue;
         }

         config.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      }

      NodeList acceptorNodes = e.getElementsByTagName("acceptor");

      for (int i = 0; i < acceptorNodes.getLength(); i++)
      {
         Element acceptorNode = (Element)acceptorNodes.item(i);

         TransportConfiguration acceptorConfig = parseTransportConfiguration(acceptorNode, config);

         config.getAcceptorConfigurations().add(acceptorConfig);
      }

      NodeList bgNodes = e.getElementsByTagName("broadcast-group");

      for (int i = 0; i < bgNodes.getLength(); i++)
      {
         Element bgNode = (Element)bgNodes.item(i);

         parseBroadcastGroupConfiguration(bgNode, config);
      }

      NodeList dgNodes = e.getElementsByTagName("discovery-group");

      for (int i = 0; i < dgNodes.getLength(); i++)
      {
         Element dgNode = (Element)dgNodes.item(i);

         parseDiscoveryGroupConfiguration(dgNode, config);
      }

      NodeList brNodes = e.getElementsByTagName("bridge");

      for (int i = 0; i < brNodes.getLength(); i++)
      {
         Element mfNode = (Element)brNodes.item(i);

         parseBridgeConfiguration(mfNode, config);
      }

      NodeList gaNodes = e.getElementsByTagName("grouping-handler");

      for (int i = 0; i < gaNodes.getLength(); i++)
      {
         Element gaNode = (Element)gaNodes.item(i);

         parseGroupingHandlerConfiguration(gaNode, config);
      }

      NodeList ccNodes = e.getElementsByTagName("cluster-connection");

      for (int i = 0; i < ccNodes.getLength(); i++)
      {
         Element ccNode = (Element)ccNodes.item(i);

         parseClusterConnectionConfiguration(ccNode, config);
      }

      NodeList dvNodes = e.getElementsByTagName("divert");

      for (int i = 0; i < dvNodes.getLength(); i++)
      {
         Element dvNode = (Element)dvNodes.item(i);

         parseDivertConfiguration(dvNode, config);
      }

      // Persistence config

      config.setLargeMessagesDirectory(XMLConfigurationUtil.getString(e,
                                                                      "large-messages-directory",
                                                                      config.getLargeMessagesDirectory(),
                                                                      Validators.NOT_NULL_OR_EMPTY));

      config.setBindingsDirectory(XMLConfigurationUtil.getString(e,
                                                                 "bindings-directory",
                                                                 config.getBindingsDirectory(),
                                                                 Validators.NOT_NULL_OR_EMPTY));

      config.setCreateBindingsDir(XMLConfigurationUtil.getBoolean(e,
                                                                  "create-bindings-dir",
                                                                  config.isCreateBindingsDir()));

      config.setJournalDirectory(XMLConfigurationUtil.getString(e,
                                                                "journal-directory",
                                                                config.getJournalDirectory(),
                                                                Validators.NOT_NULL_OR_EMPTY));


      config.setPageMaxConcurrentIO(XMLConfigurationUtil.getInteger(e,
                                                                    "page-max-concurrent-io",
                                                                    5,
                                                                    Validators.MINUS_ONE_OR_GT_ZERO));

      config.setPagingDirectory(XMLConfigurationUtil.getString(e,
                                                               "paging-directory",
                                                               config.getPagingDirectory(),
                                                               Validators.NOT_NULL_OR_EMPTY));

      config.setCreateJournalDir(XMLConfigurationUtil.getBoolean(e, "create-journal-dir", config.isCreateJournalDir()));

      String s = XMLConfigurationUtil.getString(e,
                                                "journal-type",
                                                config.getJournalType().toString(),
                                                Validators.JOURNAL_TYPE);

      if (s.equals(JournalType.NIO.toString()))
      {
         config.setJournalType(JournalType.NIO);
      }
      else if (s.equals(JournalType.ASYNCIO.toString()))
      {
         // https://jira.jboss.org/jira/browse/HORNETQ-295
         // We do the check here to see if AIO is supported so we can use the correct defaults and/or use
         // correct settings in xml
         // If we fall back later on these settings can be ignored
         boolean supportsAIO = AIOSequentialFileFactory.isSupported();

         if (supportsAIO)
         {
            config.setJournalType(JournalType.ASYNCIO);
         }
         else
         {
            if (validateAIO)
            {
               HornetQServerLogger.LOGGER.AIONotFound();
            }

            config.setJournalType(JournalType.NIO);
         }
      }

      config.setJournalSyncTransactional(XMLConfigurationUtil.getBoolean(e,
                                                                         "journal-sync-transactional",
                                                                         config.isJournalSyncTransactional()));

      config.setJournalSyncNonTransactional(XMLConfigurationUtil.getBoolean(e,
                                                                            "journal-sync-non-transactional",
                                                                            config.isJournalSyncNonTransactional()));

      config.setJournalFileSize(XMLConfigurationUtil.getInteger(e,
                                                                "journal-file-size",
                                                                config.getJournalFileSize(),
                                                                Validators.GT_ZERO));

      int journalBufferTimeout = XMLConfigurationUtil.getInteger(e,
                                                                 "journal-buffer-timeout",
                                                                 config.getJournalType() == JournalType.ASYNCIO ? JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO
                                                                                                               : JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO,
                                                                 Validators.GT_ZERO);

       int journalBufferSize = XMLConfigurationUtil.getInteger(e,
                                                              "journal-buffer-size",
                                                              config.getJournalType() == JournalType.ASYNCIO ? JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO
                                                                                                            : JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO,
                                                              Validators.GT_ZERO);

      int journalMaxIO = XMLConfigurationUtil.getInteger(e,
                                                         "journal-max-io",
                                                         config.getJournalType() == JournalType.ASYNCIO ? HornetQDefaultConfiguration.DEFAULT_JOURNAL_MAX_IO_AIO
                                                                                                       : HornetQDefaultConfiguration.DEFAULT_JOURNAL_MAX_IO_NIO,
                                                         Validators.GT_ZERO);

      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         config.setJournalBufferTimeout_AIO(journalBufferTimeout);
         config.setJournalBufferSize_AIO(journalBufferSize);
         config.setJournalMaxIO_AIO(journalMaxIO);
      }
      else
      {
         config.setJournalBufferTimeout_NIO(journalBufferTimeout);
         config.setJournalBufferSize_NIO(journalBufferSize);
         config.setJournalMaxIO_NIO(journalMaxIO);
      }

      config.setJournalMinFiles(XMLConfigurationUtil.getInteger(e,
                                                                "journal-min-files",
                                                                config.getJournalMinFiles(),
                                                                Validators.GT_ZERO));

      config.setJournalCompactMinFiles(XMLConfigurationUtil.getInteger(e,
                                                                       "journal-compact-min-files",
                                                                       config.getJournalCompactMinFiles(),
                                                                       Validators.GE_ZERO));

      config.setJournalCompactPercentage(XMLConfigurationUtil.getInteger(e,
                                                                         "journal-compact-percentage",
                                                                         config.getJournalCompactPercentage(),
                                                                         Validators.PERCENTAGE));

      config.setLogJournalWriteRate(XMLConfigurationUtil.getBoolean(e,
                                                                    "log-journal-write-rate",
                                                                    HornetQDefaultConfiguration.DEFAULT_JOURNAL_LOG_WRITE_RATE));

      config.setJournalPerfBlastPages(XMLConfigurationUtil.getInteger(e,
                                                                      "perf-blast-pages",
                                                                      HornetQDefaultConfiguration.DEFAULT_JOURNAL_PERF_BLAST_PAGES,
                                                                      Validators.MINUS_ONE_OR_GT_ZERO));

      config.setRunSyncSpeedTest(XMLConfigurationUtil.getBoolean(e, "run-sync-speed-test", config.isRunSyncSpeedTest()));

      config.setWildcardRoutingEnabled(XMLConfigurationUtil.getBoolean(e,
                                                                       "wild-card-routing-enabled",
                                                                       config.isWildcardRoutingEnabled()));

      config.setMessageCounterEnabled(XMLConfigurationUtil.getBoolean(e,
                                                                      "message-counter-enabled",
                                                                      config.isMessageCounterEnabled()));

      config.setMessageCounterSamplePeriod(XMLConfigurationUtil.getLong(e,
                                                                        "message-counter-sample-period",
                                                                        config.getMessageCounterSamplePeriod(),
                                                                        Validators.GT_ZERO));

      config.setMessageCounterMaxDayHistory(XMLConfigurationUtil.getInteger(e,
                                                                            "message-counter-max-day-history",
                                                                            config.getMessageCounterMaxDayHistory(),
                                                                            Validators.GT_ZERO));

      config.setServerDumpInterval(XMLConfigurationUtil.getLong(e,
                                                                "server-dump-interval",
                                                                config.getServerDumpInterval(),
                                                                Validators.MINUS_ONE_OR_GT_ZERO)); // in
      // milliseconds

      config.setMemoryWarningThreshold(XMLConfigurationUtil.getInteger(e,
                                                                       "memory-warning-threshold",
                                                                       config.getMemoryWarningThreshold(),
                                                                       Validators.PERCENTAGE));

      config.setMemoryMeasureInterval(XMLConfigurationUtil.getLong(e,
                                                                   "memory-measure-interval",
                                                                   config.getMemoryMeasureInterval(),
                                                                   Validators.MINUS_ONE_OR_GT_ZERO)); // in

      parseAddressSettings(e, config);

      parseQueues(e, config);

      parseSecurity(e, config);

      NodeList connectorServiceConfigs = e.getElementsByTagName("connector-service");

      ArrayList<ConnectorServiceConfiguration> configs = new ArrayList<ConnectorServiceConfiguration>();

      for (int i = 0; i < connectorServiceConfigs.getLength(); i++)
      {
         Element node = (Element)connectorServiceConfigs.item(i);

         configs.add((parseConnectorService(node)));
      }

      config.setConnectorServiceConfigurations(configs);
   }

   /**
    * @param e
    * @param config
    */
   private void parseSecurity(final Element e, final Configuration config)
   {
      NodeList elements = e.getElementsByTagName("security-settings");

      if (elements.getLength() != 0)
      {
         Element node = (Element)elements.item(0);
         NodeList list = node.getElementsByTagName("security-setting");
         for (int i = 0; i < list.getLength(); i++)
         {
            Pair<String, Set<Role>> securityItem = parseSecurityRoles(list.item(i));
            config.getSecurityRoles().put(securityItem.getA(), securityItem.getB());
         }
      }
   }

   /**
    * @param e
    * @param config
    */
   private void parseQueues(final Element e, final Configuration config)
   {
      NodeList elements = e.getElementsByTagName("queues");

      if (elements.getLength() != 0)
      {
         Element node = (Element)elements.item(0);
         NodeList list = node.getElementsByTagName("queue");
         for (int i = 0; i < list.getLength(); i++)
         {
            CoreQueueConfiguration queueConfig = parseQueueConfiguration(list.item(i));
            config.getQueueConfigurations().add(queueConfig);
         }
      }
   }

   /**
    * @param e
    * @param config
    */
   private void parseAddressSettings(final Element e, final Configuration config)
   {
      NodeList elements = e.getElementsByTagName("address-settings");

      if (elements.getLength() != 0)
      {
         Element node = (Element)elements.item(0);
         NodeList list = node.getElementsByTagName("address-setting");
         for (int i = 0; i < list.getLength(); i++)
         {
            Pair<String, AddressSettings> addressSettings = parseAddressSettings(list.item(i));
            config.getAddressesSettings().put(addressSettings.getA(), addressSettings.getB());
         }
      }
   }

   /**
    * @param node
    * @return
    */
   protected Pair<String, Set<Role>> parseSecurityRoles(final Node node)
   {
      String match = node.getAttributes().getNamedItem("match").getNodeValue();

      HashSet<Role> securityRoles = new HashSet<Role>();

      Pair<String, Set<Role>> securityMatch = new Pair<String, Set<Role>>(match, securityRoles);

      ArrayList<String> send = new ArrayList<String>();
      ArrayList<String> consume = new ArrayList<String>();
      ArrayList<String> createDurableQueue = new ArrayList<String>();
      ArrayList<String> deleteDurableQueue = new ArrayList<String>();
      ArrayList<String> createNonDurableQueue = new ArrayList<String>();
      ArrayList<String> deleteNonDurableQueue = new ArrayList<String>();
      ArrayList<String> manageRoles = new ArrayList<String>();
      ArrayList<String> allRoles = new ArrayList<String>();
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (FileConfigurationParser.PERMISSION_ELEMENT_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            String type = child.getAttributes().getNamedItem(FileConfigurationParser.TYPE_ATTR_NAME).getNodeValue();
            String roleString = child.getAttributes()
                                     .getNamedItem(FileConfigurationParser.ROLES_ATTR_NAME)
                                     .getNodeValue();
            String[] roles = roleString.split(",");
            for (String role : roles)
            {
               if (FileConfigurationParser.SEND_NAME.equals(type))
               {
                  send.add(role.trim());
               }
               else if (FileConfigurationParser.CONSUME_NAME.equals(type))
               {
                  consume.add(role.trim());
               }
               else if (FileConfigurationParser.CREATEDURABLEQUEUE_NAME.equals(type))
               {
                  createDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.DELETEDURABLEQUEUE_NAME.equals(type))
               {
                  deleteDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.CREATE_NON_DURABLE_QUEUE_NAME.equals(type))
               {
                  createNonDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.DELETE_NON_DURABLE_QUEUE_NAME.equals(type))
               {
                  deleteNonDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.CREATETEMPQUEUE_NAME.equals(type))
               {
                  createNonDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.DELETETEMPQUEUE_NAME.equals(type))
               {
                  deleteNonDurableQueue.add(role.trim());
               }
               else if (FileConfigurationParser.MANAGE_NAME.equals(type))
               {
                  manageRoles.add(role.trim());
               }
               if (!allRoles.contains(role.trim()))
               {
                  allRoles.add(role.trim());
               }
            }
         }

      }

      for (String role : allRoles)
      {
         securityRoles.add(new Role(role,
                                    send.contains(role),
                                    consume.contains(role),
                                    createDurableQueue.contains(role),
                                    deleteDurableQueue.contains(role),
                                    createNonDurableQueue.contains(role),
                                    deleteNonDurableQueue.contains(role),
                                    manageRoles.contains(role)));
      }

      return securityMatch;
   }

   /**
    * @param node
    * @return
    */
   protected Pair<String, AddressSettings> parseAddressSettings(final Node node)
   {
      String match = node.getAttributes().getNamedItem("match").getNodeValue();

      NodeList children = node.getChildNodes();

      AddressSettings addressSettings = new AddressSettings();

      Pair<String, AddressSettings> setting = new Pair<String, AddressSettings>(match, addressSettings);

      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (FileConfigurationParser.DEAD_LETTER_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setDeadLetterAddress(queueName);
         }
         else if (FileConfigurationParser.EXPIRY_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setExpiryAddress(queueName);
         }
         else if (FileConfigurationParser.EXPIRY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setExpiryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedeliveryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.REDELIVERY_DELAY_MULTIPLIER_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedeliveryMultiplier(Double.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.MAX_REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxRedeliveryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxSizeBytes(Long.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.PAGE_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setPageSizeBytes(Long.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.PAGE_MAX_CACHE_SIZE_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setPageCacheMaxSize(Integer.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMessageCounterHistoryDayLimit(Integer.valueOf(child.getTextContent()));
         }
         else if (FileConfigurationParser.ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            String value = child.getTextContent().trim();
            Validators.ADDRESS_FULL_MESSAGE_POLICY_TYPE.validate(FileConfigurationParser.ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME,
                                                                 value);
            AddressFullMessagePolicy policy = null;
            if (value.equals(AddressFullMessagePolicy.BLOCK.toString()))
            {
               policy = AddressFullMessagePolicy.BLOCK;
            }
            else if (value.equals(AddressFullMessagePolicy.DROP.toString()))
            {
               policy = AddressFullMessagePolicy.DROP;
            }
            else if (value.equals(AddressFullMessagePolicy.PAGE.toString()))
            {
               policy = AddressFullMessagePolicy.PAGE;
            }
            else if (value.equals(AddressFullMessagePolicy.FAIL.toString()))
            {
               policy = AddressFullMessagePolicy.FAIL;
            }
            addressSettings.setAddressFullMessagePolicy(policy);
         }
         else if (FileConfigurationParser.LVQ_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setLastValueQueue(Boolean.valueOf(child.getTextContent().trim()));
         }
         else if (FileConfigurationParser.MAX_DELIVERY_ATTEMPTS.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxDeliveryAttempts(Integer.valueOf(child.getTextContent().trim()));
         }
         else if (FileConfigurationParser.REDISTRIBUTION_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedistributionDelay(Long.valueOf(child.getTextContent().trim()));
         }
         else if (FileConfigurationParser.SEND_TO_DLA_ON_NO_ROUTE.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setSendToDLAOnNoRoute(Boolean.valueOf(child.getTextContent().trim()));
         }
      }
      return setting;
   }

   protected CoreQueueConfiguration parseQueueConfiguration(final Node node)
   {
      String name = node.getAttributes().getNamedItem("name").getNodeValue();
      String address = null;
      String filterString = null;
      boolean durable = true;

      NodeList children = node.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("address"))
         {
            address = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
         else if (child.getNodeName().equals("durable"))
         {
            durable = Boolean.parseBoolean(child.getTextContent().trim());
         }
      }

      return new CoreQueueConfiguration(address, name, filterString, durable);
   }

   private TransportConfiguration parseTransportConfiguration(final Element e, final Configuration mainConfig)
   {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String clazz = XMLConfigurationUtil.getString(e, "factory-class", null, Validators.NOT_NULL_OR_EMPTY);

      Map<String, Object> params = new HashMap<String, Object>();

      if (mainConfig.isMaskPassword())
      {
         params.put(HornetQDefaultConfiguration.PROP_MASK_PASSWORD, mainConfig.isMaskPassword());

         if (mainConfig.getPasswordCodec() != null)
         {
            params.put(HornetQDefaultConfiguration.PROP_PASSWORD_CODEC, mainConfig.getPasswordCodec());
         }
      }

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

   private void parseBroadcastGroupConfiguration(final Element e, final Configuration mainConfig)
   {
      String name = e.getAttribute("name");

      List<String> connectorNames = new ArrayList<String>();

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("connector-ref"))
         {
            String connectorName = XMLConfigurationUtil.getString(e,
               "connector-ref",
               null,
               Validators.NOT_NULL_OR_EMPTY);

            connectorNames.add(connectorName);
         }
      }

      long broadcastPeriod = XMLConfigurationUtil.getLong(e,
         "broadcast-period",
         HornetQDefaultConfiguration.DEFAULT_BROADCAST_PERIOD,
         Validators.GT_ZERO);

      String localAddress = XMLConfigurationUtil.getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = XMLConfigurationUtil.getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = XMLConfigurationUtil.getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = XMLConfigurationUtil.getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = XMLConfigurationUtil.getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = XMLConfigurationUtil.getString(e, "jgroups-channel", null, Validators.NO_CHECK);


      // TODO: validate if either jgroups or UDP is being filled

      BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration;

      if (jgroupsFile != null)
      {
         endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jgroupsFile, jgroupsChannel);
      }
      else
      {
         endpointFactoryConfiguration = new UDPBroadcastGroupConfiguration(groupAddress, groupPort, localAddress, localBindPort);
      }

      BroadcastGroupConfiguration config = new BroadcastGroupConfiguration(name, broadcastPeriod, connectorNames, endpointFactoryConfiguration);

      mainConfig.getBroadcastGroupConfigurations().add(config);
   }

   private void parseDiscoveryGroupConfiguration(final Element e, final Configuration mainConfig)
   {
      String name = e.getAttribute("name");

      long discoveryInitialWaitTimeout = XMLConfigurationUtil.getLong(e,
         "initial-wait-timeout",
         HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
         Validators.GT_ZERO);

      long refreshTimeout = XMLConfigurationUtil.getLong(e,
         "refresh-timeout",
         HornetQDefaultConfiguration.DEFAULT_BROADCAST_REFRESH_TIMEOUT,
         Validators.GT_ZERO);

      String localBindAddress = XMLConfigurationUtil.getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = XMLConfigurationUtil.getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = XMLConfigurationUtil.getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = XMLConfigurationUtil.getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = XMLConfigurationUtil.getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = XMLConfigurationUtil.getString(e, "jgroups-channel", null, Validators.NO_CHECK);

      // TODO: validate if either jgroups or UDP is being filled
      BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration;
      if (jgroupsFile != null)
      {
         endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jgroupsFile, jgroupsChannel);
      }
      else
      {
         endpointFactoryConfiguration = new UDPBroadcastGroupConfiguration(groupAddress, groupPort, localBindAddress, localBindPort);
      }

      DiscoveryGroupConfiguration config = new DiscoveryGroupConfiguration(name, refreshTimeout, discoveryInitialWaitTimeout, endpointFactoryConfiguration);

      if (mainConfig.getDiscoveryGroupConfigurations().containsKey(name))
      {
         HornetQServerLogger.LOGGER.discoveryGroupAlreadyDeployed(name);

         return;
      }
      else
      {
         mainConfig.getDiscoveryGroupConfigurations().put(name, config);
      }
   }

   private void parseClusterConnectionConfiguration(final Element e, final Configuration mainConfig)
   {
      String name = e.getAttribute("name");

      String address = XMLConfigurationUtil.getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String connectorName = XMLConfigurationUtil.getString(e, "connector-ref", null, Validators.NOT_NULL_OR_EMPTY);

      boolean duplicateDetection = XMLConfigurationUtil.getBoolean(e,
                                                                   "use-duplicate-detection",
                                                                   HornetQDefaultConfiguration.DEFAULT_CLUSTER_DUPLICATE_DETECTION);

      boolean forwardWhenNoConsumers = XMLConfigurationUtil.getBoolean(e,
                                                                       "forward-when-no-consumers",
                                                                       HornetQDefaultConfiguration.DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS);

      int maxHops = XMLConfigurationUtil.getInteger(e,
                                                    "max-hops",
                                                    HornetQDefaultConfiguration.DEFAULT_CLUSTER_MAX_HOPS,
                                                    Validators.GE_ZERO);

      long clientFailureCheckPeriod = XMLConfigurationUtil.getLong(e, "check-period",
                                                                   HornetQDefaultConfiguration.DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD, Validators.GT_ZERO) ;

      long connectionTTL = XMLConfigurationUtil.getLong(e, "connection-ttl",
                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_CONNECTION_TTL, Validators.GT_ZERO) ;


      long retryInterval = XMLConfigurationUtil.getLong(e,
                                                        "retry-interval",
                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_RETRY_INTERVAL,
                                                        Validators.GT_ZERO);

      long callTimeout = XMLConfigurationUtil.getLong(e, "call-timeout", HornetQClient.DEFAULT_CALL_TIMEOUT, Validators.GT_ZERO);

      long callFailoverTimeout = XMLConfigurationUtil.getLong(e, "call-failover-timeout", HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, Validators.MINUS_ONE_OR_GT_ZERO);

      double retryIntervalMultiplier = XMLConfigurationUtil.getDouble(e, "retry-interval-multiplier",
                                                                      HornetQDefaultConfiguration.DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER, Validators.GT_ZERO);

      int minLargeMessageSize = XMLConfigurationUtil.getInteger(e, "min-large-message-size", HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, Validators.GT_ZERO);

      long maxRetryInterval = XMLConfigurationUtil.getLong(e, "max-retry-interval", HornetQDefaultConfiguration.DEFAULT_CLUSTER_MAX_RETRY_INTERVAL, Validators.GT_ZERO);

      int reconnectAttempts = XMLConfigurationUtil.getInteger(e, "reconnect-attempts", HornetQDefaultConfiguration.DEFAULT_CLUSTER_RECONNECT_ATTEMPTS, Validators.MINUS_ONE_OR_GE_ZERO);


      int confirmationWindowSize = XMLConfigurationUtil.getInteger(e,
                                                                   "confirmation-window-size",
                                                                   FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                   Validators.GT_ZERO);

      String discoveryGroupName = null;

      List<String> staticConnectorNames = new ArrayList<String>();

      boolean allowDirectConnectionsOnly = false;

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("discovery-group-ref"))
         {
            discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();
         }
         else if (child.getNodeName().equals("static-connectors"))
         {
            Node attr = child.getAttributes().getNamedItem("allow-direct-connections-only");
            if (attr != null)
            {
               allowDirectConnectionsOnly = "true".equalsIgnoreCase(attr.getNodeValue()) || allowDirectConnectionsOnly;
            }
            getStaticConnectors(staticConnectorNames, child);
         }
      }

      ClusterConnectionConfiguration config;

      if (discoveryGroupName == null)
      {
         config =
                  new ClusterConnectionConfiguration(name, address, connectorName,
                                                     minLargeMessageSize, clientFailureCheckPeriod, connectionTTL,
                                                     retryInterval, retryIntervalMultiplier, maxRetryInterval,
                                                     reconnectAttempts, callTimeout, callFailoverTimeout,
                                                     duplicateDetection, forwardWhenNoConsumers, maxHops,
                                                     confirmationWindowSize,
                                                     staticConnectorNames,
                                                     allowDirectConnectionsOnly);
      }
      else
      {
         config =
                  new ClusterConnectionConfiguration(name, address, connectorName,
                                                     minLargeMessageSize, clientFailureCheckPeriod,
                                                     connectionTTL,
                                                     retryInterval,
                                                     retryIntervalMultiplier,
                                                     maxRetryInterval,
                                                     reconnectAttempts,
                                                     callTimeout,
                                                     callFailoverTimeout,
                                                     duplicateDetection,
                                                     forwardWhenNoConsumers,
                                                     maxHops,
                                                     confirmationWindowSize,
                                                     discoveryGroupName);
      }

      mainConfig.getClusterConfigurations().add(config);
   }

   private void parseGroupingHandlerConfiguration(final Element node, final Configuration mainConfiguration)
   {
      String name = node.getAttribute("name");
      String type = XMLConfigurationUtil.getString(node, "type", null, Validators.NOT_NULL_OR_EMPTY);
      String address = XMLConfigurationUtil.getString(node, "address", null, Validators.NOT_NULL_OR_EMPTY);
      Integer timeout = XMLConfigurationUtil.getInteger(node,
                                                        "timeout",
                                                        GroupingHandlerConfiguration.DEFAULT_TIMEOUT,
                                                        Validators.GT_ZERO);
      mainConfiguration.setGroupingHandlerConfiguration(new GroupingHandlerConfiguration(new SimpleString(name),
                                                                                         type.equals(GroupingHandlerConfiguration.TYPE.LOCAL.getType()) ? GroupingHandlerConfiguration.TYPE.LOCAL
                                                                                                                                                       : GroupingHandlerConfiguration.TYPE.REMOTE,
                                                                                         new SimpleString(address),
                                                                                         timeout));
   }

   private void parseBridgeConfiguration(final Element brNode, final Configuration mainConfig) throws Exception
   {
      String name = brNode.getAttribute("name");

      String queueName = XMLConfigurationUtil.getString(brNode, "queue-name", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = XMLConfigurationUtil.getString(brNode, "forwarding-address", null, Validators.NO_CHECK);

      String transformerClassName = XMLConfigurationUtil.getString(brNode,
                                                                   "transformer-class-name",
                                                                   null,
                                                                   Validators.NO_CHECK);

       // Default bridge conf
      int confirmationWindowSize = XMLConfigurationUtil.getInteger(brNode,
                                                                   "confirmation-window-size",
                                                                   FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                                   Validators.GT_ZERO);

      long retryInterval = XMLConfigurationUtil.getLong(brNode,
                                                        "retry-interval",
                                                        HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                        Validators.GT_ZERO);

      long clientFailureCheckPeriod = XMLConfigurationUtil.getLong(brNode, "check-period",
                                                                   HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, Validators.GT_ZERO) ;

      long connectionTTL = XMLConfigurationUtil.getLong(brNode, "connection-ttl",
                                                        HornetQClient.DEFAULT_CONNECTION_TTL, Validators.GT_ZERO) ;

      int minLargeMessageSize = XMLConfigurationUtil.getInteger(brNode,
                                                                "min-large-message-size",
                                                                HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                Validators.GT_ZERO);

      long maxRetryInterval = XMLConfigurationUtil.getLong(brNode, "max-retry-interval", HornetQClient.DEFAULT_MAX_RETRY_INTERVAL, Validators.GT_ZERO);


      double retryIntervalMultiplier = XMLConfigurationUtil.getDouble(brNode,
                                                                      "retry-interval-multiplier",
                                                                      HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                      Validators.GT_ZERO);

      int reconnectAttempts = XMLConfigurationUtil.getInteger(brNode,
                                                              "reconnect-attempts",
                                                              HornetQDefaultConfiguration.DEFAULT_BRIDGE_RECONNECT_ATTEMPTS,
                                                              Validators.MINUS_ONE_OR_GE_ZERO);

      boolean useDuplicateDetection = XMLConfigurationUtil.getBoolean(brNode,
                                                                      "use-duplicate-detection",
                                                                      HornetQDefaultConfiguration.DEFAULT_BRIDGE_DUPLICATE_DETECTION);

      String user = XMLConfigurationUtil.getString(brNode,
                                                   "user",
                                                   HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                   Validators.NO_CHECK);

      NodeList clusterPassNodes = brNode.getElementsByTagName("password");
      String password = null;
      boolean maskPassword = mainConfig.isMaskPassword();

      SensitiveDataCodec<String> codec = null;

      if (clusterPassNodes.getLength() > 0)
      {
         Node passNode = clusterPassNodes.item(0);
         password = passNode.getTextContent();
      }

      if (password != null)
      {
         if (maskPassword)
         {
            codec = PasswordMaskingUtil.getCodec(mainConfig.getPasswordCodec());
            password = codec.decode(password);
         }
      }
      else
      {
         password = HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD;
      }

      boolean ha = XMLConfigurationUtil.getBoolean(brNode, "ha", false);

      String filterString = null;

      List<String> staticConnectorNames = new ArrayList<String>();

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
         else if (child.getNodeName().equals("static-connectors"))
         {
            getStaticConnectors(staticConnectorNames, child);
         }
      }

      BridgeConfiguration config;

      if (!staticConnectorNames.isEmpty())
      {
         config = new BridgeConfiguration(name,
                                          queueName,
                                          forwardingAddress,
                                          filterString,
                                          transformerClassName,
                                          minLargeMessageSize,
                                          clientFailureCheckPeriod,
                                          connectionTTL,
                                          retryInterval,
                                          maxRetryInterval,
                                          retryIntervalMultiplier,
                                          reconnectAttempts,
                                          useDuplicateDetection,
                                          confirmationWindowSize,
                                          staticConnectorNames,
                                          ha,
                                          user,
                                          password);
      }
      else
      {
         config = new BridgeConfiguration(name,
                                          queueName,
                                          forwardingAddress,
                                          filterString,
                                          transformerClassName,
                                          minLargeMessageSize,
                                          clientFailureCheckPeriod,
                                          connectionTTL,
                                          retryInterval,
                                          maxRetryInterval,
                                          retryIntervalMultiplier,
                                          reconnectAttempts,
                                          useDuplicateDetection,
                                          confirmationWindowSize,
                                          discoveryGroupName,
                                          ha,
                                          user,
                                          password);
      }

      mainConfig.getBridgeConfigurations().add(config);
   }

   private void getStaticConnectors(List<String> staticConnectorNames, Node child)
   {
      NodeList children2 = ((Element)child).getElementsByTagName("connector-ref");

      for (int k = 0; k < children2.getLength(); k++)
      {
         Element child2 = (Element)children2.item(k);

         String connectorName = child2.getChildNodes().item(0).getNodeValue();

         staticConnectorNames.add(connectorName);
      }
   }

   private void parseDivertConfiguration(final Element e, final Configuration mainConfig)
   {
      String name = e.getAttribute("name");

      String routingName = XMLConfigurationUtil.getString(e, "routing-name", null, Validators.NO_CHECK);

      String address = XMLConfigurationUtil.getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = XMLConfigurationUtil.getString(e,
                                                                "forwarding-address",
                                                                null,
                                                                Validators.NOT_NULL_OR_EMPTY);

      boolean exclusive = XMLConfigurationUtil.getBoolean(e, "exclusive", HornetQDefaultConfiguration.DEFAULT_DIVERT_EXCLUSIVE);

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

      mainConfig.getDivertConfigurations().add(config);
   }

   private ConnectorServiceConfiguration parseConnectorService(final Element e)
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

      return new ConnectorServiceConfiguration(clazz, params, name);
   }

   // Inner classes -------------------------------------------------

}

