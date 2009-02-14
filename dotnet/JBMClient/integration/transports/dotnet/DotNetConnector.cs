using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JBoss.JBM.Client.integration.transports.dotnet
{
    using System.Collections.Generic;
    using JBoss.JBM.Client.remoting;
    using JBoss.JBM.Client.remoting.spi;

    public class DotNetConnector : Connector
    {
        #region Properties

        public Dictionary<string, object> Configuration
        {
            get;
            private set;
        }

        public BufferHandler Handler
        {
            get;
            private set;
        }

        public ConnectionLifeCycleListener Listener
        {
            get;
            private set;
        }

        #endregion

        #region Constructors

        public DotNetConnector(Dictionary<string, object> _configuration, BufferHandler _handler, ConnectionLifeCycleListener _listener)
        {
            this.Configuration = _configuration;
            this.Handler = _handler;
            this.Listener = _listener;
        }

        #endregion

        #region Connector Members

        public void Start()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public bool IsStarted()
        {
            throw new NotImplementedException();
        }

        public Connection CreateConnection()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
