package org.hornetq.spi.core.remoting;

import java.util.Map;

/**
 * Abstract connector
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public abstract class AbstractConnector implements Connector
{
   protected final Map<String, Object> configuration;

   protected AbstractConnector(Map<String, Object> configuration)
   {
      this.configuration = configuration;
   }
}
