package org.jboss.messaging.core.remoting.impl.netty;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;

/**
 * A NettyConnectorFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NettyConnectorFactory implements ConnectorFactory
{
   public Connector createConnector(final Location location, final ConnectionParams params,
         final RemotingHandler handler, final ConnectionLifeCycleListener listener)
   {
      return new NettyConnector(location, params, handler, listener);
   }
}
