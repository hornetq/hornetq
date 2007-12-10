/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ConnectorManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

import java.util.List;
import java.util.Map;

/**
 * A deployable JBoss Messaging connection factory.
 * 
 * The default connection factory does not support load balancing or
 * automatic failover.
 * 
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactory
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   private static final Logger log = Logger.getLogger(ConnectionFactory.class);
   // Attributes -----------------------------------------------------------------------------------

   private String clientID;
   
   private List<String> jndiBindings;
   
   private int prefetchSize = 150;
   
   private boolean slowConsumers;
   
   private boolean supportsFailover;
   
   private boolean supportsLoadBalancing;
   
   private LoadBalancingFactory loadBalancingFactory;
   
   private int defaultTempQueueFullSize = 200000;
   
   private int defaultTempQueuePageSize = 2000;
   
   private int defaultTempQueueDownCacheSize = 2000;
   
   private int dupsOKBatchSize = 1000;

   private ServerPeer serverPeer;
   
   private ConnectionFactoryManager connectionFactoryManager;
   
   private ConnectorManager connectorManager;
   
   private ConnectionManager connectionManager;
      
   private Connector connector;

   private boolean started;

   private boolean strictTck;
   
   private boolean disableRemotingChecks;

   private String name;
   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactory()
   {
      this(null);
   }

   public ConnectionFactory(String clientID)
   {
      this.clientID = clientID;

      // by default, a clustered connection uses a round-robin load balancing policy
      this.loadBalancingFactory = LoadBalancingFactory.getDefaultFactory();
   }

   // ServiceMBeanSupport overrides ----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      try
      {
         log.debug(this + " starting");

         started = true;
         
         if (connector == null)
         {
            throw new IllegalArgumentException("A Connector must be specified for " +
                                               "each Connection Factory");
         }
         
         if (serverPeer == null)
         {
            throw new IllegalArgumentException("ServerPeer must be specified for " +
                                               "each Connection Factory");
         }
      
         String locatorURI = connector.getInvokerLocator();

         if (!serverPeer.isSupportsFailover())
         {
            this.supportsFailover = false;
         }
         
         InvokerLocator locator = new InvokerLocator(locatorURI);
         
         String protocol = locator.getProtocol();
         
         if (!disableRemotingChecks && (protocol.equals("bisocket") || protocol.equals("sslbisocket")))
         {         
	         //Sanity check - If users are using the AS Service Binding Manager to provide the remoting connector
	         //configuration, it is quite easy for them to end up using an old version depending on what version on 
	         //the AS they are running in - e.g. if they have forgotten to update it.
	         //This can lead to subtle errors - therefore we do a sanity check by checking the existence of some properties
	         //which should always be there
	         Map params = locator.getParameters();	         
	         
	         //The "compulsory" parameters
	         boolean cont =
	         	checkParam(params, "marshaller", "org.jboss.jms.wireformat.JMSWireFormat") &&	         		
		         checkParam(params, "unmarshaller", "org.jboss.jms.wireformat.JMSWireFormat") &&
		         checkParam(params, "dataType", "jms") &&
		         checkParam(params, "timeout", "0") &&
		         checkParam(params, "clientSocketClass", "org.jboss.jms.client.remoting.ClientSocketWrapper") &&
		         checkParam(params, "numberOfCallRetries", "1") &&
		         checkParam(params, "pingFrequency", "214748364") &&
		         checkParam(params, "pingWindowFactor", "10");
	         
	         if (!cont)
	         {
	         	throw new IllegalArgumentException("Failed to deploy connection factory since remoting configuration seems incorrect.");	         			                            
	         }

	         String val = (String)params.get("clientLeasePeriod");	  
	         if (val != null)
	         {
		         int i = Integer.parseInt(val);
		         if (i < 5000)
		         {
		         	log.warn("Value of clientLeasePeriod at " + i + " seems low. Normal values are >= 5000");
		         }
	         }
	         
	         val = (String)params.get("clientMaxPoolSize");	
	         if (val != null)
	         {
		         int i = Integer.parseInt(val);
		         if (i < 50)
		         {
		         	log.warn("Value of clientMaxPoolSize at " + i + " seems low. Normal values are >= 50");
		         }
	         }
         }

         connectionFactoryManager = serverPeer.getConnectionFactoryManager();
         connectorManager = serverPeer.getConnectorManager();
         connectionManager = serverPeer.getConnectionManager();

          int refCount = connectorManager.registerConnector(getName());

         long leasePeriod = connector.getLeasePeriod();
         // if leasePeriod <= 0, disable pinging altogether

         boolean enablePing = leasePeriod > 0;
         
         if (refCount == 1 && enablePing)
         {
            // TODO Something is not quite right here, we can detect failure even if pinging is not
            // enabled, for example if we try to send a callback to the client and sending the
            // calback fails

            // install the connection listener that listens for failed connections            
            connector.addConnectionListener((ConnectionListener) connectionManager);
         }
         
         // We use the MBean service name to uniquely identify the connection factory
         
         connectionFactoryManager.
            registerConnectionFactory(getName(), clientID, jndiBindings,
                                      locatorURI, enablePing, prefetchSize, slowConsumers,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,                                      
                                      defaultTempQueueDownCacheSize, dupsOKBatchSize, supportsFailover, supportsLoadBalancing,
                                      loadBalancingFactory, strictTck);               
         
         String info = "Connector " + locator.getProtocol() + "://" +
            locator.getHost() + ":" + locator.getPort();
                 
         if (enablePing)
         {
            info += " has leasing enabled, lease period " + leasePeriod + " milliseconds";
         }
         else
         {
            info += " has lease disabled";
         }
      
         log.info(info);
         log.info(this + " started");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
   }
   
   public synchronized void stop() throws Exception
   {
      try
      {
         started = false;
         
         connectionFactoryManager.
            unregisterConnectionFactory(getName(), supportsFailover, supportsLoadBalancing);
         connectorManager.unregisterConnector(getName());
         
         log.info(this + " undeployed");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      } 
   }

   // JMX managed attributes -----------------------------------------------------------------------
   
   public int getDefaultTempQueueFullSize()
   {
      return defaultTempQueueFullSize;
   }
   
   public void setDefaultTempQueueFullSize(int size)
   {
      this.defaultTempQueueFullSize = size;
   }
   
   public int getDefaultTempQueuePageSize()
   {
      return defaultTempQueuePageSize;
   }
   
   public void setDefaultTempQueuePageSize(int size)
   {
      this.defaultTempQueuePageSize = size;
   }
   
   public int getDefaultTempQueueDownCacheSize()
   {
      return defaultTempQueueDownCacheSize;
   }
   
   public void setDefaultTempQueueDownCacheSize(int size)
   {
      this.defaultTempQueueDownCacheSize = size;
   }
   
   public int getPrefetchSize()
   {
      return prefetchSize;
   }
   
   public void setPrefetchSize(int prefetchSize)
   {
      this.prefetchSize = prefetchSize;
   }
   
   public boolean isSlowConsumers()
   {
   	return slowConsumers;
   }
   
   public void setSlowConsumers(boolean slowConsumers)
   {
   	this.slowConsumers = slowConsumers;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setJNDIBindings(List<String> bindings) throws Exception
   {
      jndiBindings = bindings;
   }

   public List<String> getJNDIBindings()
   {
      if (jndiBindings == null)
      {
         return null;
      }
      return jndiBindings;
   }

   public ServerPeer getServerPeer()
    {
        return serverPeer;
    }

    public void setServerPeer(ServerPeer serverPeer)
    {
        this.serverPeer = serverPeer;
    }

    public Connector getConnector()
    {
        return connector;
    }

    public void setConnector(Connector connector)
    {
        this.connector = connector;
    }

    public boolean isSupportsFailover()
   {
      return supportsFailover;
   }
   
   public void setSupportsFailover(boolean supportsFailover)
   {
      if (started)
      {
         log.warn("supportsFailover can only be changed when connection factory is stopped");
         return;
      }
      this.supportsFailover = supportsFailover;
   }
   
   public boolean isSupportsLoadBalancing()
   {
      return supportsLoadBalancing;
   }
   
   public void setSupportsLoadBalancing(boolean supportsLoadBalancing)
   {
      if (started)
      {
         log.warn("supportsLoadBalancing can only be changed when connection factory is stopped");
         return;
      }
      this.supportsLoadBalancing = supportsLoadBalancing;
   }

   public String getLoadBalancingFactory()
   {
      return loadBalancingFactory.getClass().getName();
   }

   public void setLoadBalancingFactory(String factoryName) throws Exception
   {
      if (started)
      {
         log.warn("Load balancing policy can only be changed when connection factory is stopped");
         return;
      }
      
      //We don't use Class.forName() since then it won't work with scoped deployments
      Class clz = Thread.currentThread().getContextClassLoader().loadClass(factoryName);
      
      loadBalancingFactory = (LoadBalancingFactory)clz.newInstance();
   }
   
   public void setDupsOKBatchSize(int size) throws Exception
   {
      if (started)
      {
         log.warn("DupsOKBatchSize can only be changed when connection factory is stopped");
         return;
      }

      this.dupsOKBatchSize = size;
   }

   public int getDupsOKBatchSize()
   {
   	return dupsOKBatchSize;
   }

   public boolean isStrictTck()
   {
   	return strictTck;
   }

   public void setStrictTck(boolean strictTck)
   {
      if (started)
      {
         log.warn("StrictTCK can only be changed when connection factory is stopped");
         return;         
      }
      
   	this.strictTck = strictTck;
   }
   
   public boolean isDisableRemotingChecks()
   {
   	return disableRemotingChecks;
   }
   
   public void setDisableRemotingChecks(boolean disable)
   {
      if (started)
      {
         log.warn("DisableRemotingChecks can only be changed when connection factory is stopped");
         return;
      }
      
   	this.disableRemotingChecks = disable;
   }

   public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

   // JMX managed operations -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private boolean checkParam(Map params, String key, String value)
   {
   	String val = (String)params.get(key);
   	if (val == null)
   	{
   		log.error("Parameter " + key + " is not specified in the remoting congiguration");
   		return false;
   	}   	
   	else if (!val.equals(value))
   	{
   		log.error("Parameter " + key + " has a different value ( " + val + ") to the default shipped with this version of " +
   				    "JBM (" + value + "). " +
   				    "There is rarely a valid reason to change this parameter value. " +
   				    "If you are using ServiceBindingManager to supply the remoting configuration you should check " +
   				    "that the parameter value specified there exactly matches the value in the configuration supplied with JBM. " +
   				    "This connection factory will now not deploy. To override these checks set 'disableRemotingChecks' to " +
   				    "true on the connection factory. Only do this if you are absolutely sure you know the consequences.");
   		return false;
   	}
   	else
   	{
   		return true;
   	}
   }
   
   // Inner classes --------------------------------------------------------------------------------
}
