package org.jboss.messaging.core.remoting.impl.netty;

import java.util.Map;

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
   private static final long serialVersionUID = 5230517134523506528L;

   public Connector createConnector(final Map<String, Object> configuration,
                                    final BufferHandler handler,
                                    final ConnectionLifeCycleListener listener)
   {
      return new NettyConnector(configuration, handler, listener);
   }
}
