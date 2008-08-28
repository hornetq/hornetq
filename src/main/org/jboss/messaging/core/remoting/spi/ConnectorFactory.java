package org.jboss.messaging.core.remoting.spi;

import java.io.Serializable;
import java.util.Map;

import org.jboss.messaging.core.remoting.RemotingHandler;

/**
 * 
 * A ConnectorFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ConnectorFactory extends Serializable
{
   Connector createConnector(Map<String, Object> configuration,
                             RemotingHandler handler,
                             ConnectionLifeCycleListener listener);
}
