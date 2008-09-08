package org.jboss.messaging.core.remoting.impl.mina;

import java.util.Map;

import org.jboss.messaging.core.remoting.spi.BufferHandler;
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
   public Connector createConnector(final Map<String, Object> configuration,
                                    final BufferHandler handler,
                                    final ConnectionLifeCycleListener listener)
   {
      return new MinaConnector(configuration, handler, listener);
   }

}
