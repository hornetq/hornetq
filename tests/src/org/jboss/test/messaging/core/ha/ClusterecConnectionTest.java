package org.jboss.test.messaging.core.ha;

import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClusteredClientConnectionFactoryDelegate;
import org.jboss.jms.client.state.ConnectionState;

/**
 * Start two JBoss instances (clustered) to run these tests.
 */
public class ClusterecConnectionTest extends HATestBase
{


   public void setup() throws Exception
   {
      super.setUp("/HAConnectionFactory");
   }

   public void testSimpleConnection() throws Exception
   {
      JBossConnectionFactory factory = (JBossConnectionFactory)this.ctx1.lookup("/HAConnectionFactory");
      JBossConnection conn = (JBossConnection) factory.createConnection();

      ClusteredClientConnectionFactoryDelegate delegate = (ClusteredClientConnectionFactoryDelegate)factory.getDelegate();

      assertEquals(2,delegate.getDelegates().length);

      Destination destination = (Destination)getCtx1().lookup("topic/testTopic");

      for (int i=0;i<100;i++)
      {
         JBossConnection conn2 = (JBossConnection)factory.createConnection();
         conn2.setClientID("client" + i);
         conn2.start();
         Session session = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(destination); 
         ConnectionState state = (ConnectionState) ((ClientConnectionDelegate)conn2.getDelegate()).getState();
         log.info("state.serverId=" + state.getServerID());
         //conn2.close();
      }

      System.out.println("****************** finish *********************");


      Thread.sleep(120000);
   }
}
