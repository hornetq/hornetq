using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JBoss.JBM.Client.integration.transports.dotnet
{
    using JBoss.JBM.Client.remoting.spi;

    public class DotNetConnectorFactory : ConnectorFactory
    {

        #region ConnectorFactory Members

        public Connector createConnector(Dictionary<string, object> configuration, BufferHandler handler, ConnectionLifeCycleListener listener)
        {
            return new DotNetConnector(configuration, handler, listener);
        }

        #endregion
    }
}
