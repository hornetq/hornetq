package org.jboss.messaging.core.remoting.impl.mina;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;

/**
 * 
 * A MinaConnectorFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MinaConnectorFactory implements ConnectorFactory
{
   public Connector createConnector(final Location location, final ConnectionParams params,
         final RemotingHandler handler, final ConnectionLifeCycleListener listener)
   {
      return new MinaConnector(location, params, handler, listener);
   }

}
