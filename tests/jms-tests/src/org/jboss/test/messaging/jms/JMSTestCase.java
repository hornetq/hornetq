package org.jboss.test.messaging.jms;

import javax.naming.InitialContext;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
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

      getJmsServerManager().createConnectionFactory("testsuitecf",
                                                    new TransportConfiguration("org.jboss.messaging.core.remoting.impl.netty.NettyConnectorFactory"),
                                                    null,
                                                    ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,                                                       
                                                    ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                    null,
                                                    ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                    ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                    ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP_ID,
                                                    ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                    "/testsuitecf");
      
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
