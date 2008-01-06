/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import java.util.List;
import java.util.Map;

import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.util.ExceptionUtil;

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
   
   private ConnectionManager connectionManager;
      
   private MinaService minaService;

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
         
         if (minaService == null)
         {
            throw new IllegalArgumentException("A MinaService must be specified for " +
                                               "each Connection Factory");
         }
         
         if (serverPeer == null)
         {
            throw new IllegalArgumentException("ServerPeer must be specified for " +
                                               "each Connection Factory");
         }
      
         ServerLocator serverLocator = minaService.getLocator();

         if (!serverPeer.isSupportsFailover())
         {
            this.supportsFailover = false;
         }
         
         connectionFactoryManager = serverPeer.getConnectionFactoryManager();
         connectionManager = serverPeer.getConnectionManager();

         // We use the MBean service name to uniquely identify the connection factory
         
         connectionFactoryManager.
            registerConnectionFactory(getName(), clientID, jndiBindings,
                                      serverLocator.getURI(), false, prefetchSize, slowConsumers,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,                                      
                                      defaultTempQueueDownCacheSize, dupsOKBatchSize, supportsFailover, supportsLoadBalancing,
                                      loadBalancingFactory, strictTck);               
      
         log.info("Server locator is " + serverLocator);
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
    
    public void setMinaService(MinaService service)
    {
       this.minaService = service;
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
