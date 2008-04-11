package org.jboss.test.messaging.jms;

import javax.naming.InitialContext;

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
      cf = getConnectionFactory();
   }


   public JMSTestCase(String name)
   {
      super(name);
   }

}
