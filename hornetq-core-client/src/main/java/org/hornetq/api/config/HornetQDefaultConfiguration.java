package org.hornetq.api.config;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.impl.JournalConstants;

/**
 * Default values of HornetQ configuration parameters.
 */
public final class HornetQDefaultConfiguration
{
   /*
    * <p> In order to avoid compile time in-lining of constants, all access is done through methods
    * and all fields are PRIVATE STATIC but not FINAL. This is done following the recommendation at
    * <a href="http://docs.oracle.com/javase/specs/jls/se7/html/jls-13.html#jls-13.4.9">13.4.9.
    * final Fields and Constants</a>
    * @see http://docs.oracle.com/javase/specs/jls/se7/html/jls-13.html#jls-13.4.9
    */

   private HornetQDefaultConfiguration()
   {
      // Utility class
   }

   public static long getDefaultClientFailureCheckPeriod()
   {
      return DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
   }

   public static long getDefaultConnectionTtl()
   {
      return DEFAULT_CONNECTION_TTL;
   }

   public static double getDefaultRetryIntervalMultiplier()
   {
      return DEFAULT_RETRY_INTERVAL_MULTIPLIER;
   }

   public static long getDefaultMaxRetryInterval()
   {
      return DEFAULT_MAX_RETRY_INTERVAL;
   }

   public static String getDefaultJmxDomain()
   {
      return DEFAULT_JMX_DOMAIN;
   }

   public static boolean isDefaultPersistDeliveryCountBeforeDelivery()
   {
      return DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY;
   }

   public static boolean isDefaultBackup()
   {
      return DEFAULT_BACKUP;
   }

   public static boolean isDefaultAllowAutoFailback()
   {
      return DEFAULT_ALLOW_AUTO_FAILBACK;
   }

   public static boolean isDefaultSharedStore()
   {
      return DEFAULT_SHARED_STORE;
   }

   public static boolean isDefaultFileDeploymentEnabled()
   {
      return DEFAULT_FILE_DEPLOYMENT_ENABLED;
   }

   public static boolean isDefaultPersistenceEnabled()
   {
      return DEFAULT_PERSISTENCE_ENABLED;
   }

   public static long getDefaultFileDeployerScanPeriod()
   {
      return DEFAULT_FILE_DEPLOYER_SCAN_PERIOD;
   }

   public static int getDefaultScheduledThreadPoolMaxSize()
   {
      return DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
   }

   public static int getDefaultThreadPoolMaxSize()
   {
      return DEFAULT_THREAD_POOL_MAX_SIZE;
   }

   public static long getDefaultSecurityInvalidationInterval()
   {
      return DEFAULT_SECURITY_INVALIDATION_INTERVAL;
   }

   public static boolean isDefaultSecurityEnabled()
   {
      return DEFAULT_SECURITY_ENABLED;
   }

   public static boolean isDefaultJmxManagementEnabled()
   {
      return DEFAULT_JMX_MANAGEMENT_ENABLED;
   }

   public static long getDefaultConnectionTtlOverride()
   {
      return DEFAULT_CONNECTION_TTL_OVERRIDE;
   }

   public static boolean isDefaultAsyncConnectionExecutionEnabled()
   {
      return DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED;
   }

   public static String getDefaultBindingsDirectory()
   {
      return DEFAULT_BINDINGS_DIRECTORY;
   }

   public static boolean isDefaultCreateBindingsDir()
   {
      return DEFAULT_CREATE_BINDINGS_DIR;
   }

   public static String getDefaultJournalDir()
   {
      return DEFAULT_JOURNAL_DIR;
   }

   public static String getDefaultPagingDir()
   {
      return DEFAULT_PAGING_DIR;
   }

   public static String getDefaultLargeMessagesDir()
   {
      return DEFAULT_LARGE_MESSAGES_DIR;
   }

   public static int getDefaultMaxConcurrentPageIo()
   {
      return DEFAULT_MAX_CONCURRENT_PAGE_IO;
   }

   public static boolean isDefaultCreateJournalDir()
   {
      return DEFAULT_CREATE_JOURNAL_DIR;
   }

   public static boolean isDefaultJournalSyncTransactional()
   {
      return DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;
   }

   public static boolean isDefaultJournalSyncNonTransactional()
   {
      return DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;
   }

   public static int getDefaultJournalFileSize()
   {
      return DEFAULT_JOURNAL_FILE_SIZE;
   }

   public static int getDefaultJournalCompactMinFiles()
   {
      return DEFAULT_JOURNAL_COMPACT_MIN_FILES;
   }

   public static int getDefaultJournalCompactPercentage()
   {
      return DEFAULT_JOURNAL_COMPACT_PERCENTAGE;
   }

   public static int getDefaultJournalMinFiles()
   {
      return DEFAULT_JOURNAL_MIN_FILES;
   }

   public static int getDefaultJournalMaxIoAio()
   {
      return DEFAULT_JOURNAL_MAX_IO_AIO;
   }

   public static int getDefaultJournalBufferTimeoutAio()
   {
      return DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;
   }

   public static int getDefaultJournalBufferSizeAio()
   {
      return DEFAULT_JOURNAL_BUFFER_SIZE_AIO;
   }

   public static int getDefaultJournalMaxIoNio()
   {
      return DEFAULT_JOURNAL_MAX_IO_NIO;
   }

   public static int getDefaultJournalBufferTimeoutNio()
   {
      return DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;
   }

   public static int getDefaultJournalBufferSizeNio()
   {
      return DEFAULT_JOURNAL_BUFFER_SIZE_NIO;
   }

   public static boolean isDefaultJournalLogWriteRate()
   {
      return DEFAULT_JOURNAL_LOG_WRITE_RATE;
   }

   public static int getDefaultJournalPerfBlastPages()
   {
      return DEFAULT_JOURNAL_PERF_BLAST_PAGES;
   }

   public static boolean isDefaultRunSyncSpeedTest()
   {
      return DEFAULT_RUN_SYNC_SPEED_TEST;
   }

   public static boolean isDefaultWildcardRoutingEnabled()
   {
      return DEFAULT_WILDCARD_ROUTING_ENABLED;
   }

   public static boolean isDefaultMessageCounterEnabled()
   {
      return DEFAULT_MESSAGE_COUNTER_ENABLED;
   }

   public static long getDefaultMessageCounterSamplePeriod()
   {
      return DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;
   }

   public static int getDefaultMessageCounterMaxDayHistory()
   {
      return DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;
   }

   public static long getDefaultTransactionTimeout()
   {
      return DEFAULT_TRANSACTION_TIMEOUT;
   }

   public static long getDefaultTransactionTimeoutScanPeriod()
   {
      return DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD;
   }

   public static SimpleString getDefaultManagementAddress()
   {
      return DEFAULT_MANAGEMENT_ADDRESS;
   }

   public static SimpleString getDefaultManagementNotificationAddress()
   {
      return DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;
   }

   public static String getDefaultClusterUser()
   {
      return DEFAULT_CLUSTER_USER;
   }

   public static String getDefaultClusterPassword()
   {
      return DEFAULT_CLUSTER_PASSWORD;
   }

   public static long getDefaultBroadcastPeriod()
   {
      return DEFAULT_BROADCAST_PERIOD;
   }

   public static long getDefaultBroadcastRefreshTimeout()
   {
      return DEFAULT_BROADCAST_REFRESH_TIMEOUT;
   }

   public static long getDefaultMessageExpiryScanPeriod()
   {
      return DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD;
   }

   public static int getDefaultMessageExpiryThreadPriority()
   {
      return DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY;
   }

   public static int getDefaultIdCacheSize()
   {
      return DEFAULT_ID_CACHE_SIZE;
   }

   public static boolean isDefaultPersistIdCache()
   {
      return DEFAULT_PERSIST_ID_CACHE;
   }

   public static boolean isDefaultClusterDuplicateDetection()
   {
      return DEFAULT_CLUSTER_DUPLICATE_DETECTION;
   }

   public static boolean isDefaultClusterForwardWhenNoConsumers()
   {
      return DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS;
   }

   public static int getDefaultClusterMaxHops()
   {
      return DEFAULT_CLUSTER_MAX_HOPS;
   }

   public static long getDefaultClusterRetryInterval()
   {
      return DEFAULT_CLUSTER_RETRY_INTERVAL;
   }

   public static int getDefaultClusterReconnectAttempts()
   {
      return DEFAULT_CLUSTER_RECONNECT_ATTEMPTS;
   }

   public static long getDefaultClusterFailureCheckPeriod()
   {
      return DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD;
   }

   public static long getDefaultClusterConnectionTtl()
   {
      return DEFAULT_CLUSTER_CONNECTION_TTL;
   }

   public static double getDefaultClusterRetryIntervalMultiplier()
   {
      return DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER;
   }

   public static long getDefaultClusterMaxRetryInterval()
   {
      return DEFAULT_CLUSTER_MAX_RETRY_INTERVAL;
   }

   public static boolean isDefaultDivertExclusive()
   {
      return DEFAULT_DIVERT_EXCLUSIVE;
   }

   public static boolean isDefaultBridgeDuplicateDetection()
   {
      return DEFAULT_BRIDGE_DUPLICATE_DETECTION;
   }

   public static int getDefaultBridgeReconnectAttempts()
   {
      return DEFAULT_BRIDGE_RECONNECT_ATTEMPTS;
   }

   public static long getDefaultServerDumpInterval()
   {
      return DEFAULT_SERVER_DUMP_INTERVAL;
   }

   public static boolean isDefaultFailoverOnServerShutdown()
   {
      return DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
   }

   public static int getDefaultMemoryWarningThreshold()
   {
      return DEFAULT_MEMORY_WARNING_THRESHOLD;
   }

   public static long getDefaultMemoryMeasureInterval()
   {
      return DEFAULT_MEMORY_MEASURE_INTERVAL;
   }

   public static long getDefaultFailbackDelay()
   {
      return DEFAULT_FAILBACK_DELAY;
   }

   public static boolean isDefaultCheckForLiveServer()
   {
      return DEFAULT_CHECK_FOR_LIVE_SERVER;
   }

   public static boolean isDefaultMaskPassword()
   {
      return DEFAULT_MASK_PASSWORD;
   }

   public static long getDefaultClusterNotificationInterval()
   {
      return DEFAULT_CLUSTER_NOTIFICATION_INTERVAL;
   }

   public static int getDefaultClusterNotificationAttempts()
   {
      return DEFAULT_CLUSTER_NOTIFICATION_ATTEMPTS;
   }

   public static String getPropMaskPassword()
   {
      return PROP_MASK_PASSWORD;
   }

   public static String getPropPasswordCodec()
   {
      return PROP_PASSWORD_CODEC;
   }

   public static int getDefaultBridgeConnectSameNode()
   {
       return DEFAULT_BRIDGE_CONNECT_SAME_NODE;
   }

   //shared by client and core/server
   private static long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = 30000;

   // 1 minute - this should be higher than ping period

   private static long DEFAULT_CONNECTION_TTL = 1 * 60 * 1000;

   private static double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   private static long DEFAULT_MAX_RETRY_INTERVAL = 2000;

   private static String DEFAULT_JMX_DOMAIN = "org.hornetq";
   /** Used by the JBoss-AS integration code. */
   private static boolean DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY = false;
   /** Used by the JBoss-AS integration code. */
   private static boolean DEFAULT_BACKUP = false;
   /** Used by the JBoss-AS integration code. */
   private static boolean DEFAULT_ALLOW_AUTO_FAILBACK = true;
   /** Used by the JBoss-AS integration code. */
   private static boolean DEFAULT_SHARED_STORE = true;
   private static boolean DEFAULT_FILE_DEPLOYMENT_ENABLED = false;
   /** Used by the JBoss-AS integration code. */
   private static boolean DEFAULT_PERSISTENCE_ENABLED = true;
   private static long DEFAULT_FILE_DEPLOYER_SCAN_PERIOD = 5000;
   private static int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;
   private static int DEFAULT_THREAD_POOL_MAX_SIZE = 30;
   private static long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;
   private static boolean DEFAULT_SECURITY_ENABLED = true;
   private static boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;
   private static long DEFAULT_CONNECTION_TTL_OVERRIDE = -1;
   private static boolean DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED = true;
   private static String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";
   private static boolean DEFAULT_CREATE_BINDINGS_DIR = true;
   private static String DEFAULT_JOURNAL_DIR = "data/journal";
   private static String DEFAULT_PAGING_DIR = "data/paging";
   private static String DEFAULT_LARGE_MESSAGES_DIR = "data/largemessages";
   private static int DEFAULT_MAX_CONCURRENT_PAGE_IO = 5;
   private static boolean DEFAULT_CREATE_JOURNAL_DIR = true;
   private static boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;
   private static boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = true;
   private static int DEFAULT_JOURNAL_FILE_SIZE = 10485760;
   private static int DEFAULT_JOURNAL_COMPACT_MIN_FILES = 10;
   private static int DEFAULT_JOURNAL_COMPACT_PERCENTAGE = 30;
   private static int DEFAULT_JOURNAL_MIN_FILES = 2;
   private static int DEFAULT_JOURNAL_MAX_IO_AIO = 500;
   private static int DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;
   private static int DEFAULT_JOURNAL_BUFFER_SIZE_AIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO;
   private static int DEFAULT_JOURNAL_MAX_IO_NIO = 1;
   private static int DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;
   private static int DEFAULT_JOURNAL_BUFFER_SIZE_NIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO;
   private static boolean DEFAULT_JOURNAL_LOG_WRITE_RATE = false;
   private static int DEFAULT_JOURNAL_PERF_BLAST_PAGES = -1;
   private static boolean DEFAULT_RUN_SYNC_SPEED_TEST = false;
   private static boolean DEFAULT_WILDCARD_ROUTING_ENABLED = true;
   private static boolean DEFAULT_MESSAGE_COUNTER_ENABLED = false;
   private static long DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD = 10000;
   private static int DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY = 10;
   private static long DEFAULT_TRANSACTION_TIMEOUT = 300000; // 5 minutes
   private static long DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD = 1000;
   /**
    * The management address is prefixed with {@literal jms.queue} so that JMS clients can send
    * messages to it too.
    */
   private static SimpleString DEFAULT_MANAGEMENT_ADDRESS = new SimpleString("jms.queue.hornetq.management");
   private static SimpleString DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS = new SimpleString("hornetq.notifications");
   private static String DEFAULT_CLUSTER_USER = "HORNETQ.CLUSTER.ADMIN.USER";
   private static String DEFAULT_CLUSTER_PASSWORD = "CHANGE ME!!";
   private static long DEFAULT_BROADCAST_PERIOD = 2000;
   private static long DEFAULT_BROADCAST_REFRESH_TIMEOUT = 10000;
   private static long DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD = 30000;
   private static int DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY = 3;
   private static int DEFAULT_ID_CACHE_SIZE = 20000;
   private static boolean DEFAULT_PERSIST_ID_CACHE = true;
   private static boolean DEFAULT_CLUSTER_DUPLICATE_DETECTION = true;
   private static boolean DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS = false;
   private static int DEFAULT_CLUSTER_MAX_HOPS = 1;
   private static long DEFAULT_CLUSTER_RETRY_INTERVAL = 500;
   private static int DEFAULT_CLUSTER_RECONNECT_ATTEMPTS = -1;
   private static long DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD = getDefaultClientFailureCheckPeriod();
   private static long DEFAULT_CLUSTER_CONNECTION_TTL = getDefaultConnectionTtl();
   private static double DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER = getDefaultRetryIntervalMultiplier();
   private static long DEFAULT_CLUSTER_MAX_RETRY_INTERVAL = getDefaultMaxRetryInterval();
   private static boolean DEFAULT_DIVERT_EXCLUSIVE = false;
   private static boolean DEFAULT_BRIDGE_DUPLICATE_DETECTION = true;
   private static int DEFAULT_BRIDGE_RECONNECT_ATTEMPTS = -1;
   private static int DEFAULT_BRIDGE_CONNECT_SAME_NODE = 10;
   private static long DEFAULT_SERVER_DUMP_INTERVAL = -1;
   private static boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;
   private static int DEFAULT_MEMORY_WARNING_THRESHOLD = 25;
   private static long DEFAULT_MEMORY_MEASURE_INTERVAL = -1; // in milliseconds
   private static long DEFAULT_FAILBACK_DELAY = 5000; // in milliseconds
   private static boolean DEFAULT_CHECK_FOR_LIVE_SERVER = false;
   private static boolean DEFAULT_MASK_PASSWORD = false;
   private static long DEFAULT_CLUSTER_NOTIFICATION_INTERVAL = 1000;
   private static int DEFAULT_CLUSTER_NOTIFICATION_ATTEMPTS = 2;

   //properties passed to acceptor/connectors.
   private static String PROP_MASK_PASSWORD = "hornetq.usemaskedpassword";
   private static String PROP_PASSWORD_CODEC = "hornetq.passwordcodec";
}
