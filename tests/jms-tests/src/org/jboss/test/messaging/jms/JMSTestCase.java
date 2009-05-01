package org.jboss.test.messaging.jms;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.utils.Pair;
import org.jboss.test.messaging.JBMServerTestCase;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>23 Jul 2007
 *          <p/>
 *          $Id: $
 */
public class JMSTestCase extends JBMServerTestCase
{

   protected static JBossConnectionFactory cf;

   protected static InitialContext ic;

   protected static final String defaultConf = "all";

   protected static String conf;

   protected String overrideConf;

   protected boolean startMessagingServer = true;

   protected void setUp() throws Exception
   {
      super.setUp();

      ic = getInitialContext();

      // All jms tests should use a specific cg which has blockOnAcknowledge = true and
      // both np and p messages are sent synchronously

      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"),
                                                                                    null));

      List<String> jndiBindings = new ArrayList<String>();
      jndiBindings.add("/testsuitecf");

      getJmsServerManager().createConnectionFactory("testsuitecf",
                                                    connectorConfigs,
                                                    null,
                                                    DEFAULT_PING_PERIOD,
                                                    DEFAULT_CONNECTION_TTL,
                                                    DEFAULT_CALL_TIMEOUT,
                                                    DEFAULT_MAX_CONNECTIONS,
                                                    DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    DEFAULT_CONSUMER_MAX_RATE,
                                                    DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    DEFAULT_AUTO_GROUP,
                                                    DEFAULT_PRE_ACKNOWLEDGE,
                                                    DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    DEFAULT_ACK_BATCH_SIZE,
                                                    DEFAULT_ACK_BATCH_SIZE,
                                                    DEFAULT_USE_GLOBAL_POOLS,
                                                    DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    DEFAULT_THREAD_POOL_MAX_SIZE,                                                     
                                                    DEFAULT_RETRY_INTERVAL,
                                                    DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    DEFAULT_RECONNECT_ATTEMPTS,
                                                    DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                    jndiBindings);

      cf = (JBossConnectionFactory)getInitialContext().lookup("/testsuitecf");

      assertRemainingMessages(0);
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      getJmsServerManager().destroyConnectionFactory("testsuitecf");
      cf = null;

      assertRemainingMessages(0);
   }
}
