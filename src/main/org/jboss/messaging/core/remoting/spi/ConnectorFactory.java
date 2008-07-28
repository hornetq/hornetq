package org.jboss.messaging.core.remoting.spi;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.remoting.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.RemotingHandler;

/**
 * 
 * A ConnectorFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ConnectorFactory
{
   Connector createConnector(Location location, ConnectionParams params, 
                             RemotingHandler handler,
                             ConnectionLifeCycleListener listener);
}
