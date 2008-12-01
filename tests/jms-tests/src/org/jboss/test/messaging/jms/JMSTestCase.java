package org.jboss.test.messaging.jms;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_ON_FAILURE;

import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.util.Pair;
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

      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = 
         new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"), null));
      
      List<String> jndiBindings = new ArrayList<String>();
      jndiBindings.add("/testsuitecf");
      
      
      getJmsServerManager().createConnectionFactory("testsuitecf",
                                                    connectorConfigs,
                                                    ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,                                                       
                                                    ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                    null,
                                                    ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                    ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                    ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    true,
                                                    true,
                                                    true,
                                                    ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                    ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                    ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE,
                                                    DEFAULT_RETRY_ON_FAILURE,
                                                    DEFAULT_RETRY_INTERVAL,
                                                    DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    DEFAULT_MAX_RETRIES,
                                                    jndiBindings);
      
      cf = (JBossConnectionFactory)getInitialContext().lookup("/testsuitecf");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      getJmsServerManager().destroyConnectionFactory("testsuitecf");
      cf = null;
   }

   public JMSTestCase(String name)
   {
      super(name);
   }

}
