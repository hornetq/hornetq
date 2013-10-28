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
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.JGroupsBroadcastGroupConfiguration;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.BridgeConfiguration;
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
import org.hornetq.utils.PasswordMaskingUtil;
import org.hornetq.utils.SensitiveDataCodec;
import org.hornetq.utils.XMLConfigurationUtil;
import org.hornetq.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Parses an XML document according to the {@literal hornetq-configuration.xsd} schema.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public final class FileConfigurationParser extends XMLConfigurationUtil
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

      config.setName(getString(e, "name", config.getName(), Validators.NO_CHECK));

      NodeList elems = e.getElementsByTagName("clustered");
      if (elems != null && elems.getLength() > 0)
      {
         HornetQServerLogger.LOGGER.deprecatedConfigurationOption("clustered");

      }

      config.setCheckForLiveServer(getBoolean(e, "check-for-live-server", config.isClustered()));

      config.setAllowAutoFailBack(getBoolean(e, "allow-failback", config.isClustered()));

      config.setBackupGroupName(getString(e, "backup-group-name", config.getBackupGroupName(),
                                                               Validators.NO_CHECK));

      config.setFailbackDelay(getLong(e, "failback-delay", config.getFailbackDelay(), Validators.GT_ZERO));

      config.setFailoverOnServerShutdown(getBoolean(e, "failover-on-shutdown",
                                                                         config.isFailoverOnServerShutdown()));
      config.setReplicationClustername(getString(e, "replication-clustername", null, Validators.NO_CHECK));

      config.setMaxSavedReplicatedJournalSize(getInteger(e, "max-saved-replicated-journals-size",
            config.getMaxSavedReplicatedJournalsSize(), Validators.MINUS_ONE_OR_GE_ZERO));

      config.setBackup(getBoolean(e, "backup", config.isBackup()));

      config.setSharedStore(getBoolean(e, "shared-store", config.isSharedStore()));

      // Defaults to true when using FileConfiguration
      config.setFileDeploymentEnabled(getBoolean(e, "file-deployment-enabled", config instanceof FileConfiguration));

      config.setPersistenceEnabled(getBoolean(e, "persistence-enabled",
                                                                   config.isPersistenceEnabled()));

      config.setPersistDeliveryCountBeforeDelivery(getBoolean(e, "persist-delivery-count-before-delivery",
                                                                                   config.isPersistDeliveryCountBeforeDelivery()));

      config.setScheduledThreadPoolMaxSize(getInteger(e, "scheduled-thread-pool-max-size",
                                                      config.getScheduledThreadPoolMaxSize(), Validators.GT_ZERO));

      config.setThreadPoolMaxSize(getInteger(e, "thread-pool-max-size", config.getThreadPoolMaxSize(),
                                                                  Validators.MINUS_ONE_OR_GT_ZERO));

      config.setSecurityEnabled(getBoolean(e, "security-enabled", config.isSecurityEnabled()));

      config.setJMXManagementEnabled(getBoolean(e, "jmx-management-enabled", config.isJMXManagementEnabled()));

      config.setJMXDomain(getString(e, "jmx-domain", config.getJMXDomain(), Validators.NOT_NULL_OR_EMPTY));

      config.setSecurityInvalidationInterval(getLong(e, "security-invalidation-interval",
                                                                          config.getSecurityInvalidationInterval(),
                                                                          Validators.GT_ZERO));

      config.setConnectionTTLOverride(getLong(e,
                                                                   "connection-ttl-override",
                                                                   config.getConnectionTTLOverride(),
                                                                   Validators.MINUS_ONE_OR_GT_ZERO));

      config.setEnabledAsyncConnectionExecution(getBoolean(e,
                                                                                "async-connection-execution-enabled",
                                                                                config.isAsyncConnectionExecutionEnabled()));

      config.setTransactionTimeout(getLong(e,
                                                                "transaction-timeout",
                                                                config.getTransactionTimeout(),
                                                                Validators.GT_ZERO));

      config.setTransactionTimeoutScanPeriod(getLong(e,
                                                                          "transaction-timeout-scan-period",
                                                                          config.getTransactionTimeoutScanPeriod(),
                                                                          Validators.GT_ZERO));

      config.setMessageExpiryScanPeriod(getLong(e,
                                                                     "message-expiry-scan-period",
                                                                     config.getMessageExpiryScanPeriod(),
                                                                     Validators.MINUS_ONE_OR_GT_ZERO));

      config.setMessageExpiryThreadPriority(getInteger(e,
                                                                            "message-expiry-thread-priority",
                                                                            config.getMessageExpiryThreadPriority(),
                                                                            Validators.THREAD_PRIORITY_RANGE));

      config.setIDCacheSize(getInteger(e,
                                                            "id-cache-size",
                                                            config.getIDCacheSize(),
                                                            Validators.GT_ZERO));

      config.setPersistIDCache(getBoolean(e, "persist-id-cache", config.isPersistIDCache()));

      config.setManagementAddress(new SimpleString(getString(e,
                                                                                  "management-address",
                                                                                  config.getManagementAddress()
                                                                                        .toString(),
                                                                                  Validators.NOT_NULL_OR_EMPTY)));

      config.setManagementNotificationAddress(new SimpleString(getString(e,
                                                                                              "management-notification-address",
                                                                                              config.getManagementNotificationAddress()
                                                                                                    .toString(),
                                                                                              Validators.NOT_NULL_OR_EMPTY)));

      config.setMaskPassword(getBoolean(e, "mask-password", false));

      config.setPasswordCodec(getString(e, "password-codec", DefaultSensitiveStringCodec.class.getName(),
                                                                   Validators.NOT_NULL_OR_EMPTY));

      // parsing cluster password
      String passwordText = getString(e, "cluster-password", null, Validators.NO_CHECK);

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

      config.setClusterUser(getString(e,
                                                           "cluster-user",
                                                           config.getClusterUser(),
                                                           Validators.NO_CHECK));

      NodeList interceptorNodes = e.getElementsByTagName("remoting-interceptors");

      ArrayList<String> incomingInterceptorList = new ArrayList<String>();

      if (interceptorNodes.getLength() > 0)
      {
         NodeList interceptors = interceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++)
         {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName()))
            {
               String clazz = getTrimmedTextContent(interceptors.item(i));

               incomingInterceptorList.add(clazz);
            }
         }
      }

      NodeList incomingInterceptorNodes = e.getElementsByTagName("remoting-incoming-interceptors");

      if (incomingInterceptorNodes.getLength() > 0)
      {
         NodeList interceptors = incomingInterceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++)
         {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName()))
            {
               String clazz = getTrimmedTextContent(interceptors.item(i));

               incomingInterceptorList.add(clazz);
            }
         }
      }

      config.setIncomingInterceptorClassNames(incomingInterceptorList);

      NodeList outgoingInterceptorNodes = e.getElementsByTagName("remoting-outgoing-interceptors");

      ArrayList<String> outgoingInterceptorList = new ArrayList<String>();

      if (outgoingInterceptorNodes.getLength() > 0)
      {
         NodeList interceptors = outgoingInterceptorNodes.item(0).getChildNodes();

         for (int i = 0; i < interceptors.getLength(); i++)
         {
            if ("class-name".equalsIgnoreCase(interceptors.item(i).getNodeName()))
            {
               String clazz = interceptors.item(i).getTextContent();

               outgoingInterceptorList.add(clazz);
            }
         }
      }

      config.setOutgoingInterceptorClassNames(outgoingInterceptorList);

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

      config.setLargeMessagesDirectory(getString(e,
                                                                      "large-messages-directory",
                                                                      config.getLargeMessagesDirectory(),
                                                                      Validators.NOT_NULL_OR_EMPTY));

      config.setBindingsDirectory(getString(e,
                                                                 "bindings-directory",
                                                                 config.getBindingsDirectory(),
                                                                 Validators.NOT_NULL_OR_EMPTY));

      config.setCreateBindingsDir(getBoolean(e,
                                                                  "create-bindings-dir",
                                                                  config.isCreateBindingsDir()));

      config.setJournalDirectory(getString(e, "journal-directory", config.getJournalDirectory(),
                                           Validators.NOT_NULL_OR_EMPTY));


      config.setPageMaxConcurrentIO(getInteger(e,
                                                                    "page-max-concurrent-io",
                                                                    config.getPageMaxConcurrentIO(),
                                                                    Validators.MINUS_ONE_OR_GT_ZERO));

      config.setPagingDirectory(getString(e,
                                                               "paging-directory",
                                                               config.getPagingDirectory(),
                                                               Validators.NOT_NULL_OR_EMPTY));

      config.setCreateJournalDir(getBoolean(e, "create-journal-dir", config.isCreateJournalDir()));

      String s = getString(e,
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

      config.setJournalSyncTransactional(getBoolean(e,
                                                                         "journal-sync-transactional",
                                                                         config.isJournalSyncTransactional()));

      config.setJournalSyncNonTransactional(getBoolean(e,
                                                                            "journal-sync-non-transactional",
                                                                            config.isJournalSyncNonTransactional()));

      config.setJournalFileSize(getInteger(e,
                                                                "journal-file-size",
                                                                config.getJournalFileSize(),
                                                                Validators.GT_ZERO));

      int journalBufferTimeout = getInteger(e,
                                                                 "journal-buffer-timeout",
                                                                 config.getJournalType() == JournalType.ASYNCIO ? JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO
                                                                                                               : JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO,
                                                                 Validators.GT_ZERO);

       int journalBufferSize = getInteger(e,
                                                              "journal-buffer-size",
                                                              config.getJournalType() == JournalType.ASYNCIO ? JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO
                                                                                                            : JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO,
                                                              Validators.GT_ZERO);

      int journalMaxIO = getInteger(e,
                                                         "journal-max-io",
                                                         config.getJournalType() == JournalType.ASYNCIO ? HornetQDefaultConfiguration.getDefaultJournalMaxIoAio()
                                                                                                       : HornetQDefaultConfiguration.getDefaultJournalMaxIoNio(),
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

      config.setJournalMinFiles(getInteger(e, "journal-min-files", config.getJournalMinFiles(), Validators.GT_ZERO));

      config.setJournalCompactMinFiles(getInteger(e, "journal-compact-min-files", config.getJournalCompactMinFiles(),
                                                  Validators.GE_ZERO));

      config.setJournalCompactPercentage(getInteger(e,
                                                                         "journal-compact-percentage",
                                                                         config.getJournalCompactPercentage(),
                                                                         Validators.PERCENTAGE));

      config.setLogJournalWriteRate(getBoolean(e,
                                                                    "log-journal-write-rate",
                                                                    HornetQDefaultConfiguration.isDefaultJournalLogWriteRate()));

      config.setJournalPerfBlastPages(getInteger(e,
                                                                      "perf-blast-pages",
                                                                      HornetQDefaultConfiguration.getDefaultJournalPerfBlastPages(),
                                                                      Validators.MINUS_ONE_OR_GT_ZERO));

      config.setRunSyncSpeedTest(getBoolean(e, "run-sync-speed-test", config.isRunSyncSpeedTest()));

      config.setWildcardRoutingEnabled(getBoolean(e, "wild-card-routing-enabled", config.isWildcardRoutingEnabled()));

      config.setMessageCounterEnabled(getBoolean(e, "message-counter-enabled", config.isMessageCounterEnabled()));

      config.setMessageCounterSamplePeriod(getLong(e, "message-counter-sample-period",
                                                   config.getMessageCounterSamplePeriod(),
                                                                        Validators.GT_ZERO));

      config.setMessageCounterMaxDayHistory(getInteger(e, "message-counter-max-day-history",
                                                                            config.getMessageCounterMaxDayHistory(),
                                                                            Validators.GT_ZERO));

      config.setServerDumpInterval(getLong(e, "server-dump-interval", config.getServerDumpInterval(),
                                           Validators.MINUS_ONE_OR_GT_ZERO)); // in
      // milliseconds

      config.setMemoryWarningThreshold(getInteger(e,
                                                                       "memory-warning-threshold",
                                                                       config.getMemoryWarningThreshold(),
                                                                       Validators.PERCENTAGE));

      config.setMemoryMeasureInterval(getLong(e,
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
         NodeList list = node.getElementsByTagName(SECURITY_ELEMENT_NAME);
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
      final String match = node.getAttributes().getNamedItem("match").getNodeValue();

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
         final String name = child.getNodeName();
         if (PERMISSION_ELEMENT_NAME.equalsIgnoreCase(name))
         {
            final String type = getAttributeValue(child, TYPE_ATTR_NAME);
            final String roleString = getAttributeValue(child, ROLES_ATTR_NAME);
            String[] roles = roleString.split(",");
            for (String role : roles)
            {
               if (SEND_NAME.equals(type))
               {
                  send.add(role.trim());
               }
               else if (CONSUME_NAME.equals(type))
               {
                  consume.add(role.trim());
               }
               else if (CREATEDURABLEQUEUE_NAME.equals(type))
               {
                  createDurableQueue.add(role.trim());
               }
               else if (DELETEDURABLEQUEUE_NAME.equals(type))
               {
                  deleteDurableQueue.add(role.trim());
               }
               else if (CREATE_NON_DURABLE_QUEUE_NAME.equals(type))
               {
                  createNonDurableQueue.add(role.trim());
               }
               else if (DELETE_NON_DURABLE_QUEUE_NAME.equals(type))
               {
                  deleteNonDurableQueue.add(role.trim());
               }
               else if (CREATETEMPQUEUE_NAME.equals(type))
               {
                  createNonDurableQueue.add(role.trim());
               }
               else if (DELETETEMPQUEUE_NAME.equals(type))
               {
                  deleteNonDurableQueue.add(role.trim());
               }
               else if (MANAGE_NAME.equals(type))
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
      String match = getAttributeValue(node, "match");

      NodeList children = node.getChildNodes();

      AddressSettings addressSettings = new AddressSettings();

      Pair<String, AddressSettings> setting = new Pair<String, AddressSettings>(match, addressSettings);

      for (int i = 0; i < children.getLength(); i++)
      {
         final Node child = children.item(i);
         final String name = child.getNodeName();
         if (DEAD_LETTER_ADDRESS_NODE_NAME.equalsIgnoreCase(name))
         {
            SimpleString queueName = new SimpleString(getTrimmedTextContent(child));
            addressSettings.setDeadLetterAddress(queueName);
         }
         else if (EXPIRY_ADDRESS_NODE_NAME.equalsIgnoreCase(name))
         {
            SimpleString queueName = new SimpleString(getTrimmedTextContent(child));
            addressSettings.setExpiryAddress(queueName);
         }
         else if (EXPIRY_DELAY_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setExpiryDelay(XMLUtil.parseLong(child));
         }
         else if (REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setRedeliveryDelay(XMLUtil.parseLong(child));
         }
         else if (REDELIVERY_DELAY_MULTIPLIER_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setRedeliveryMultiplier(XMLUtil.parseDouble(child));
         }
         else if (MAX_REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setMaxRedeliveryDelay(XMLUtil.parseLong(child));
         }
         else if (MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setMaxSizeBytes(XMLUtil.parseLong(child));
         }
         else if (PAGE_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setPageSizeBytes(XMLUtil.parseLong(child));
         }
         else if (PAGE_MAX_CACHE_SIZE_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setPageCacheMaxSize(XMLUtil.parseInt(child));
         }
         else if (MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setMessageCounterHistoryDayLimit(XMLUtil.parseInt(child));
         }
         else if (ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME.equalsIgnoreCase(name))
         {
            String value = getTrimmedTextContent(child);
            Validators.ADDRESS_FULL_MESSAGE_POLICY_TYPE.validate(ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME,
                                                                 value);
            AddressFullMessagePolicy policy = Enum.valueOf(AddressFullMessagePolicy.class, value);
            addressSettings.setAddressFullMessagePolicy(policy);
         }
         else if (LVQ_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setLastValueQueue(XMLUtil.parseBoolean(child));
         }
         else if (MAX_DELIVERY_ATTEMPTS.equalsIgnoreCase(name))
         {
            addressSettings.setMaxDeliveryAttempts(XMLUtil.parseInt(child));
         }
         else if (REDISTRIBUTION_DELAY_NODE_NAME.equalsIgnoreCase(name))
         {
            addressSettings.setRedistributionDelay(XMLUtil.parseLong(child));
         }
         else if (SEND_TO_DLA_ON_NO_ROUTE.equalsIgnoreCase(name))
         {
            addressSettings.setSendToDLAOnNoRoute(XMLUtil.parseBoolean(child));
         }
      }
      return setting;
   }

   protected CoreQueueConfiguration parseQueueConfiguration(final Node node)
   {
      String name = getAttributeValue(node, "name");
      String address = null;
      String filterString = null;
      boolean durable = true;

      NodeList children = node.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("address"))
         {
            address = getTrimmedTextContent(child);
         }
         else if (child.getNodeName().equals("filter"))
         {
            filterString = getAttributeValue(child, "string");
         }
         else if (child.getNodeName().equals("durable"))
         {
            durable = XMLUtil.parseBoolean(child);
         }
      }

      return new CoreQueueConfiguration(address, name, filterString, durable);
   }

   private TransportConfiguration parseTransportConfiguration(final Element e, final Configuration mainConfig)
   {
      Node nameNode = e.getAttributes().getNamedItem("name");

      String name = nameNode != null ? nameNode.getNodeValue() : null;

      String clazz = getString(e, "factory-class", null, Validators.NOT_NULL_OR_EMPTY);

      Map<String, Object> params = new HashMap<String, Object>();

      if (mainConfig.isMaskPassword())
      {
         params.put(HornetQDefaultConfiguration.getPropMaskPassword(), mainConfig.isMaskPassword());

         if (mainConfig.getPasswordCodec() != null)
         {
            params.put(HornetQDefaultConfiguration.getPropPasswordCodec(), mainConfig.getPasswordCodec());
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
            String connectorName = getString(e,
               "connector-ref",
               null,
               Validators.NOT_NULL_OR_EMPTY);

            connectorNames.add(connectorName);
         }
      }

      long broadcastPeriod =
               getLong(e, "broadcast-period", HornetQDefaultConfiguration.getDefaultBroadcastPeriod(), Validators.GT_ZERO);

      String localAddress = getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = getString(e, "jgroups-channel", null, Validators.NO_CHECK);


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

      long discoveryInitialWaitTimeout =
               getLong(e, "initial-wait-timeout", HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT,
                       Validators.GT_ZERO);

      long refreshTimeout =
               getLong(e, "refresh-timeout", HornetQDefaultConfiguration.getDefaultBroadcastRefreshTimeout(),
                       Validators.GT_ZERO);

      String localBindAddress = getString(e, "local-bind-address", null, Validators.NO_CHECK);

      int localBindPort = getInteger(e, "local-bind-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String groupAddress = getString(e, "group-address", null, Validators.NO_CHECK);

      int groupPort = getInteger(e, "group-port", -1, Validators.MINUS_ONE_OR_GT_ZERO);

      String jgroupsFile = getString(e, "jgroups-file", null, Validators.NO_CHECK);

      String jgroupsChannel = getString(e, "jgroups-channel", null, Validators.NO_CHECK);

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

      String address = getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String connectorName = getString(e, "connector-ref", null, Validators.NOT_NULL_OR_EMPTY);

      boolean duplicateDetection =
               getBoolean(e, "use-duplicate-detection", HornetQDefaultConfiguration.isDefaultClusterDuplicateDetection());

      boolean forwardWhenNoConsumers =
               getBoolean(e, "forward-when-no-consumers",
                          HornetQDefaultConfiguration.isDefaultClusterForwardWhenNoConsumers());

      int maxHops = getInteger(e, "max-hops",
                                                    HornetQDefaultConfiguration.getDefaultClusterMaxHops(),
                                                    Validators.GE_ZERO);

      long clientFailureCheckPeriod =
               getLong(e, "check-period", HornetQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(),
                       Validators.GT_ZERO);

      long connectionTTL =
               getLong(e, "connection-ttl", HornetQDefaultConfiguration.getDefaultClusterConnectionTtl(),
                       Validators.GT_ZERO);


      long retryInterval =
               getLong(e, "retry-interval", HornetQDefaultConfiguration.getDefaultClusterRetryInterval(),
                       Validators.GT_ZERO);

      long callTimeout = getLong(e, "call-timeout", HornetQClient.DEFAULT_CALL_TIMEOUT, Validators.GT_ZERO);

      long callFailoverTimeout = getLong(e, "call-failover-timeout", HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, Validators.MINUS_ONE_OR_GT_ZERO);

      double retryIntervalMultiplier = getDouble(e, "retry-interval-multiplier",
                                                                      HornetQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(), Validators.GT_ZERO);

      int minLargeMessageSize = getInteger(e, "min-large-message-size", HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, Validators.GT_ZERO);

      long maxRetryInterval = getLong(e, "max-retry-interval", HornetQDefaultConfiguration.getDefaultClusterMaxRetryInterval(), Validators.GT_ZERO);

      int reconnectAttempts = getInteger(e, "reconnect-attempts", HornetQDefaultConfiguration.getDefaultClusterReconnectAttempts(), Validators.MINUS_ONE_OR_GE_ZERO);


      int confirmationWindowSize =
               getInteger(e, "confirmation-window-size", FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          Validators.GT_ZERO);

      long clusterNotificationInterval = getLong(e, "notification-interval", HornetQDefaultConfiguration.getDefaultClusterNotificationInterval(), Validators.GT_ZERO);

      int clusterNotificationAttempts = getInteger(e, "notification-attempts", HornetQDefaultConfiguration.getDefaultClusterNotificationAttempts(), Validators.GT_ZERO);

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
                                                     allowDirectConnectionsOnly,
                                                     clusterNotificationInterval,
                                                     clusterNotificationAttempts);
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
                                                     discoveryGroupName,
                                                     clusterNotificationInterval,
                                                     clusterNotificationAttempts);
      }

      mainConfig.getClusterConfigurations().add(config);
   }

   private void parseGroupingHandlerConfiguration(final Element node, final Configuration mainConfiguration)
   {
      String name = node.getAttribute("name");
      String type = getString(node, "type", null, Validators.NOT_NULL_OR_EMPTY);
      String address = getString(node, "address", null, Validators.NOT_NULL_OR_EMPTY);
      Integer timeout = getInteger(node, "timeout", GroupingHandlerConfiguration.DEFAULT_TIMEOUT, Validators.GT_ZERO);
      Integer groupTimeout = getInteger(node, "group-timeout", GroupingHandlerConfiguration.DEFAULT_GROUP_TIMEOUT, Validators.MINUS_ONE_OR_GT_ZERO);
      Long reaperPeriod = getLong(node, "reaper-period", GroupingHandlerConfiguration.DEFAULT_REAPER_PERIOD, Validators.GT_ZERO);
      Integer reaperPriority = getInteger(node, "reaper-priority", GroupingHandlerConfiguration.DEFAULT_REAPER_PRIORITY, Validators.THREAD_PRIORITY_RANGE);
      mainConfiguration.setGroupingHandlerConfiguration(new GroupingHandlerConfiguration(new SimpleString(name),
                                                                                         type.equals(GroupingHandlerConfiguration.TYPE.LOCAL.getType())
                                                                                                                                                       ? GroupingHandlerConfiguration.TYPE.LOCAL
                                                                                                                                                       : GroupingHandlerConfiguration.TYPE.REMOTE,
                                                                                         new SimpleString(address),
                                                                                         timeout,
                                                                                         groupTimeout,
                                                                                         reaperPeriod,
                                                                                         reaperPriority));
   }

   private void parseBridgeConfiguration(final Element brNode, final Configuration mainConfig) throws Exception
   {
      String name = brNode.getAttribute("name");

      String queueName = getString(brNode, "queue-name", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(brNode, "forwarding-address", null, Validators.NO_CHECK);

      String transformerClassName = getString(brNode, "transformer-class-name", null, Validators.NO_CHECK);

       // Default bridge conf
      int confirmationWindowSize =
               getInteger(brNode, "confirmation-window-size", FileConfiguration.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                          Validators.GT_ZERO);

      long retryInterval = getLong(brNode, "retry-interval", HornetQClient.DEFAULT_RETRY_INTERVAL, Validators.GT_ZERO);

      long clientFailureCheckPeriod =
               getLong(brNode, "check-period", HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, Validators.GT_ZERO);

      long connectionTTL = getLong(brNode, "connection-ttl", HornetQClient.DEFAULT_CONNECTION_TTL, Validators.GT_ZERO);

      int minLargeMessageSize =
               getInteger(brNode, "min-large-message-size", HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                          Validators.GT_ZERO);

      long maxRetryInterval = getLong(brNode, "max-retry-interval", HornetQClient.DEFAULT_MAX_RETRY_INTERVAL, Validators.GT_ZERO);


      double retryIntervalMultiplier =
               getDouble(brNode, "retry-interval-multiplier", HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                         Validators.GT_ZERO);

      int reconnectAttempts =
               getInteger(brNode, "reconnect-attempts", HornetQDefaultConfiguration.getDefaultBridgeReconnectAttempts(),
                          Validators.MINUS_ONE_OR_GE_ZERO);

      int reconnectAttemptsSameNode =
               getInteger(brNode, "reconnect-attempts-same-node", HornetQDefaultConfiguration.getDefaultBridgeConnectSameNode(),
                          Validators.MINUS_ONE_OR_GE_ZERO);

      boolean useDuplicateDetection = getBoolean(brNode,
                                                                      "use-duplicate-detection",
                                                                      HornetQDefaultConfiguration.isDefaultBridgeDuplicateDetection());

      String user = getString(brNode,
                                                   "user",
                                                   HornetQDefaultConfiguration.getDefaultClusterUser(),
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
         password = HornetQDefaultConfiguration.getDefaultClusterPassword();
      }

      boolean ha = getBoolean(brNode, "ha", false);

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
                                          reconnectAttemptsSameNode,
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
                                          reconnectAttemptsSameNode,
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

      String routingName = getString(e, "routing-name", null, Validators.NO_CHECK);

      String address = getString(e, "address", null, Validators.NOT_NULL_OR_EMPTY);

      String forwardingAddress = getString(e, "forwarding-address", null, Validators.NOT_NULL_OR_EMPTY);

      boolean exclusive = getBoolean(e, "exclusive", HornetQDefaultConfiguration.isDefaultDivertExclusive());

      String transformerClassName = getString(e, "transformer-class-name", null, Validators.NO_CHECK);

      String filterString = null;

      NodeList children = e.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("filter"))
         {
            filterString = getAttributeValue(child, "string");
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

      String clazz = getString(e, "factory-class", null, Validators.NOT_NULL_OR_EMPTY);

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
}
