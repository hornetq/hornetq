package org.jboss.test.messaging.jms;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.JBMServerTestCase;

import javax.naming.InitialContext;

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
                  
      String[] args = new String[]{"invm-beans.xml", "jbm-beans.xml"};
      ic = getInitialContext();
      cf = getConnectionFactory();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      //ServerManagement.stopServerPeer();
   }

   public JMSTestCase(String name)
   {
      super(name);
   }

}
