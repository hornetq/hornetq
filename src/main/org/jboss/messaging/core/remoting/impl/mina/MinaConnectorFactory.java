package org.jboss.messaging.core.remoting.impl.mina;

import java.util.Map;

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
   private static final long serialVersionUID = -1395375418386685767L;

   public Connector createConnector(final Map<String, Object> configuration,
                                    final RemotingHandler handler, final ConnectionLifeCycleListener listener)
   {
      return new MinaConnector(configuration, handler, listener);
   }

}
