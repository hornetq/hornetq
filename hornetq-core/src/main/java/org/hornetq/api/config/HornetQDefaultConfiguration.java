package org.hornetq.api.config;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.impl.JournalConstants;

/**
 * Created by IntelliJ IDEA.
 * User: andy
 * Date: 10/11/12
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
 */
public final class HornetQDefaultConfiguration
{
   private HornetQDefaultConfiguration()
   {
      // Utility class
   }

   //shared by client and core/server
   public static final long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = 30000;

   // 1 minute - this should be higher than ping period

   public static final long DEFAULT_CONNECTION_TTL = 1 * 60 * 1000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   public static final long DEFAULT_MAX_RETRY_INTERVAL = 2000;

   public static final String DEFAULT_JMX_DOMAIN = "org.hornetq";
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY = false;
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_BACKUP = false;
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_ALLOW_AUTO_FAILBACK = true;
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_SHARED_STORE = true;
   public static final boolean DEFAULT_FILE_DEPLOYMENT_ENABLED = false;
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_PERSISTENCE_ENABLED = true;
   public static final long DEFAULT_FILE_DEPLOYER_SCAN_PERIOD = 5000;
   /**
    * Used by the JBoss-AS integration code.
    * <p>
    * <a href=
    * "https://github.com/jbossas/jboss-as/blob/master/messaging/src/main/java/org/jboss/as/messaging/CommonAttributes.java"
    * >CommonAttributes</a>
    */
   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;
   /**
    * Used by the JBoss-AS integration code.
    * "https://github.com/jbossas/jboss-as/blob/master/messaging/src/main/java/org/jboss/as/messaging/CommonAttributes.java"
    * >CommonAttributes</a>
    */
   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = 30;
   public static final long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;
   public static final boolean DEFAULT_SECURITY_ENABLED = true;
   public static final boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;
   public static final long DEFAULT_CONNECTION_TTL_OVERRIDE = -1;
   public static final boolean DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED = true;
   public static final String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";
   public static final boolean DEFAULT_CREATE_BINDINGS_DIR = true;
   public static final String DEFAULT_JOURNAL_DIR = "data/journal";
   public static final String DEFAULT_PAGING_DIR = "data/paging";
   public static final String DEFAULT_LARGE_MESSAGES_DIR = "data/largemessages";
   /** Used by the JBoss-AS integration code. */
   public static final int DEFAULT_MAX_CONCURRENT_PAGE_IO = 5;
   public static final boolean DEFAULT_CREATE_JOURNAL_DIR = true;
   public static final boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;
   public static final boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = true;
   public static final int DEFAULT_JOURNAL_FILE_SIZE = 10485760;
   public static final int DEFAULT_JOURNAL_COMPACT_MIN_FILES = 10;
   public static final int DEFAULT_JOURNAL_COMPACT_PERCENTAGE = 30;
   public static final int DEFAULT_JOURNAL_MIN_FILES = 2;
   public static final int DEFAULT_JOURNAL_MAX_IO_AIO = 500;
   /** Used by the JBoss-AS integration code. */
   public static final int DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO;
   /** Used by the JBoss-AS integration code. */
   public static final int DEFAULT_JOURNAL_BUFFER_SIZE_AIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO;
   public static final int DEFAULT_JOURNAL_MAX_IO_NIO = 1;
   /** Used by the JBoss-AS integration code. */
   public static final int DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO;
   /** Used by the JBoss-AS integration code. */
   public static final int DEFAULT_JOURNAL_BUFFER_SIZE_NIO = JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO;
   public static final boolean DEFAULT_JOURNAL_LOG_WRITE_RATE = false;
   public static final int DEFAULT_JOURNAL_PERF_BLAST_PAGES = -1;
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_RUN_SYNC_SPEED_TEST = false;
   public static final boolean DEFAULT_WILDCARD_ROUTING_ENABLED = true;
   public static final boolean DEFAULT_MESSAGE_COUNTER_ENABLED = false;
   public static final long DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD = 10000;
   public static final int DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY = 10;
   public static final long DEFAULT_TRANSACTION_TIMEOUT = 300000; // 5 minutes
   public static final long DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD = 1000;
   // the management address is prefix with jms.queue so that JMS clients can send messages to it too.
   public static final SimpleString DEFAULT_MANAGEMENT_ADDRESS = new SimpleString("jms.queue.hornetq.management");
   public static final SimpleString DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS = new SimpleString("hornetq.notifications");
   public static final String DEFAULT_CLUSTER_USER = "HORNETQ.CLUSTER.ADMIN.USER";
   public static final String DEFAULT_CLUSTER_PASSWORD = "CHANGE ME!!";
   public static final long DEFAULT_BROADCAST_PERIOD = 2000;
   public static final long DEFAULT_BROADCAST_REFRESH_TIMEOUT = 10000;
   public static final long DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD = 30000;
   public static final int DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY = 3;
   public static final int DEFAULT_ID_CACHE_SIZE = 20000;
   public static final boolean DEFAULT_PERSIST_ID_CACHE = true;
   public static final boolean DEFAULT_CLUSTER_DUPLICATE_DETECTION = true;
   public static final boolean DEFAULT_CLUSTER_FORWARD_WHEN_NO_CONSUMERS = false;
   public static final int DEFAULT_CLUSTER_MAX_HOPS = 1;
   public static final long DEFAULT_CLUSTER_RETRY_INTERVAL = 500;
   public static final int DEFAULT_CLUSTER_RECONNECT_ATTEMPTS = -1;
   public static final long DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD = DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
   public static final long DEFAULT_CLUSTER_CONNECTION_TTL = DEFAULT_CONNECTION_TTL;
   public static final double DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER = DEFAULT_RETRY_INTERVAL_MULTIPLIER;
   public static final long DEFAULT_CLUSTER_MAX_RETRY_INTERVAL = DEFAULT_MAX_RETRY_INTERVAL;
   public static final boolean DEFAULT_DIVERT_EXCLUSIVE = false;
   public static final boolean DEFAULT_BRIDGE_DUPLICATE_DETECTION = true;
   public static final int DEFAULT_BRIDGE_RECONNECT_ATTEMPTS = -1;
   public static final long DEFAULT_SERVER_DUMP_INTERVAL = -1;
   public static final boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;
   /**
    * Used by the JBoss-AS integration code.
    * <p>
    * <a href=
    * "https://github.com/jbossas/jboss-as/blob/master/messaging/src/main/java/org/jboss/as/messaging/CommonAttributes.java"
    * >CommonAttributes</a>
    */
   public static final int DEFAULT_MEMORY_WARNING_THRESHOLD = 25;
   /**
    * Used by the JBoss-AS integration code.
    * <p>
    * <a href=
    * "https://github.com/jbossas/jboss-as/blob/master/messaging/src/main/java/org/jboss/as/messaging/CommonAttributes.java"
    * >CommonAttributes</a>
    */
   public static final long DEFAULT_MEMORY_MEASURE_INTERVAL = -1; // in milliseconds
   /** Used by the JBoss-AS integration code. */
   public static final long DEFAULT_FAILBACK_DELAY = 5000; // in milliseconds
   /** Used by the JBoss-AS integration code. */
   public static final boolean DEFAULT_CHECK_FOR_LIVE_SERVER = false;
   public static final boolean DEFAULT_MASK_PASSWORD = false;



   //properties passed to acceptor/connectors.
   public static final String PROP_MASK_PASSWORD = "hornetq.usemaskedpassword";
   public static final String PROP_PASSWORD_CODEC = "hornetq.passwordcodec";
}
