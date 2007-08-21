package org.jboss.test.messaging.jms;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>23 Jul 2007
 *
 * $Id: $
 *
 */
public class JMSTestCase extends MessagingTestCase
{
   protected static Topic topic1;
   
   protected static Topic topic2;
   
   protected static Topic topic3;
   
   protected static Queue queue1;
   
   protected static Queue queue2;
   
   protected static Queue queue3;
   
   protected static Queue queue4;
   
   protected static JBossConnectionFactory cf;
   
   protected static InitialContext ic;
   
   protected static final String defaultConf = "all";
   
   protected static String conf;
   
   protected String overrideConf;
   
   protected boolean startMessagingServer = true;
   	
	protected void setUp() throws Exception
	{
		super.setUp();
		
		boolean changeServer = false;
		
		String newConf = null;
					
		if (overrideConf == null && !defaultConf.equals(conf))
		{
			//Going back to default
			changeServer = true;
			
			newConf = defaultConf;
		}
		
		if (overrideConf != null && !overrideConf.equals(conf))
		{
			//Applying new config
			changeServer = true;
			
			newConf = overrideConf;
		}		
		
		if (changeServer || !ServerManagement.isStarted(0))
		{
			log.info("Config has changed so stopping server with " + conf + " config and starting new one with " + newConf);
			
			ServerManagement.stop();
			
			if (changeServer)
			{
				conf = newConf;
			}
			
			ServerManagement.start(0, conf);
			
			deployAndLookupAdministeredObjects();			
		}
		else
		{
			log.info("Server does not need to be changed");
		}
			
      checkEmpty(queue1);
      checkEmpty(queue2);
      checkEmpty(queue3);
      checkEmpty(queue4);
      
      // Check no subscriptions left lying around
            
      checkNoSubscriptions(topic1);
      checkNoSubscriptions(topic2); 
      checkNoSubscriptions(topic3); 		
	}
	
	protected void tearDown() throws Exception
	{
		super.tearDown();
		
		//A few sanity checks
		
		if (ServerManagement.isStarted(0))
		{			
			if (checkNoMessageData())
			{
				fail("Message Data exists");
			}
		}
		
		//This will tell us if any connections have been left open
		assertEquals(0, ResourceManagerFactory.instance.size());
	}

	public JMSTestCase(String name)
	{
		super(name);
	}
	
	protected void deployAndLookupAdministeredObjects() throws Exception
	{
		ServerManagement.deployTopic("Topic1");
      ServerManagement.deployTopic("Topic2");
      ServerManagement.deployTopic("Topic3");
      ServerManagement.deployQueue("Queue1");
      ServerManagement.deployQueue("Queue2");
      ServerManagement.deployQueue("Queue3");
      ServerManagement.deployQueue("Queue4");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
      topic1 = (Topic)ic.lookup("/topic/Topic1");
      topic2 = (Topic)ic.lookup("/topic/Topic2");
      topic3 = (Topic)ic.lookup("/topic/Topic3");
      queue1 = (Queue)ic.lookup("/queue/Queue1");
      queue2 = (Queue)ic.lookup("/queue/Queue2");
      queue3 = (Queue)ic.lookup("/queue/Queue3");
      queue4 = (Queue)ic.lookup("/queue/Queue4");
	}
}
