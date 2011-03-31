import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class JmsHelper
{
   public static ConnectionFactory createConnectionFactory(String configFile) throws Exception
   {
      FileConfiguration config = new FileConfiguration();
      config.setConfigurationUrl(configFile);
      config.start();
      TransportConfiguration transport = config.getConnectorConfigurations().get("netty-connector");
      return new HornetQJMSConnectionFactory(HornetQClient.createServerLocatorWithoutHA(transport));

   }

}
