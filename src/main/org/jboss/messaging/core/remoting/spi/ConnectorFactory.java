package org.jboss.messaging.core.remoting.spi;

import java.util.Map;

/**
 * 
 * A ConnectorFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ConnectorFactory
{
   Connector createConnector(Map<String, Object> configuration, BufferHandler handler,                           
                             ConnectionLifeCycleListener listener);
}
