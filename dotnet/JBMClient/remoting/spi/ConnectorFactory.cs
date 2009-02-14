namespace JBoss.JBM.Client.remoting.spi
{

    using System.Collections.Generic;
    
    
    /**
     * 
     * A ConnectorFactory
     * 
     * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
     *
     */
    public interface ConnectorFactory
    {
        Connector createConnector(Dictionary<string, object> configuration, BufferHandler handler,
                                  ConnectionLifeCycleListener listener);
    }
}