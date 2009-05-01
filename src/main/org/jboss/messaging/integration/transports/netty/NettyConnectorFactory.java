package org.jboss.messaging.integration.transports.netty;

import java.util.Map;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;

/**
 * A NettyConnectorFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NettyConnectorFactory implements ConnectorFactory
{
   public Connector createConnector(final Map<String, Object> configuration,
                                    final BufferHandler handler,
                                    final ConnectionLifeCycleListener listener,
                                    final Executor threadPool)
   {
      return new NettyConnector(configuration, handler, listener, threadPool);
   }
}
