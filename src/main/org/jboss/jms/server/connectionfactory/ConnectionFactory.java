/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import javax.management.ObjectName;

import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ConnectorManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.remoting.InvokerLocator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * A deployable JBoss Messaging connection factory.
 * 
 * The default connection factory does not support load balancing or
 * automatic failover.
 * 
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactory extends ServiceMBeanSupport
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------

   private String clientID;
   
   private JNDIBindings jndiBindings;
   
   private int prefetchSize = 150;
   
   private boolean slowConsumers;
   
   private boolean supportsFailover;
   
   private boolean supportsLoadBalancing;
   
   private LoadBalancingFactory loadBalancingFactory;
   
   private int defaultTempQueueFullSize = 200000;
   
   private int defaultTempQueuePageSize = 2000;
   
   private int defaultTempQueueDownCacheSize = 2000;
   
   private int dupsOKBatchSize = 1000;

   private ObjectName serverPeerObjectName;
   
   private ConnectionFactoryManager connectionFactoryManager;
   
   private ConnectorManager connectorManager;
   
   private ConnectionManager connectionManager;
      
   private ObjectName connectorObjectName;

   private boolean started;

   private boolean strictTck;

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

   public synchronized void startService() throws Exception
   {
      try
      {
         log.debug(this + " starting");

         started = true;
         
         if (connectorObjectName == null)
         {
            throw new IllegalArgumentException("A Connector must be specified for " +
                                               "each Connection Factory");
         }
         
         if (serverPeerObjectName == null)
         {
            throw new IllegalArgumentException("ServerPeer must be specified for " +
                                               "each Connection Factory");
         }
      
         String locatorURI = (String)server.getAttribute(connectorObjectName, "InvokerLocator");
         ServerPeer serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");

         if (!serverPeer.isSupportsFailover())
         {
            this.supportsFailover = false;
         }

         connectionFactoryManager = serverPeer.getConnectionFactoryManager();
         connectorManager = serverPeer.getConnectorManager();
         connectionManager = serverPeer.getConnectionManager();

         int refCount = connectorManager.registerConnector(connectorObjectName.getCanonicalName());

         long leasePeriod =
            ((Long)server.getAttribute(connectorObjectName, "LeasePeriod")).longValue();

         // if leasePeriod <= 0, disable pinging altogether

         boolean enablePing = leasePeriod > 0;
         
         if (refCount == 1 && enablePing)
         {
            // TODO Something is not quite right here, we can detect failure even if pinging is not
            // enabled, for example if we try to send a callback to the client and sending the
            // calback fails

            // install the connection listener that listens for failed connections            
            server.invoke(connectorObjectName, "addConnectionListener",
                  new Object[] {connectionManager},
                  new String[] {"org.jboss.remoting.ConnectionListener"});
         }
         
         // We use the MBean service name to uniquely identify the connection factory
         
         connectionFactoryManager.
            registerConnectionFactory(getServiceName().getCanonicalName(), clientID, jndiBindings,
                                      locatorURI, enablePing, prefetchSize, slowConsumers,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,                                      
                                      defaultTempQueueDownCacheSize, dupsOKBatchSize, supportsFailover, supportsLoadBalancing,
                                      loadBalancingFactory, strictTck);
      
         InvokerLocator locator = new InvokerLocator(locatorURI);

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

   public synchronized void stopService() throws Exception
   {
      try
      {
         started = false;
         
         connectionFactoryManager.
            unregisterConnectionFactory(getServiceName().getCanonicalName(), supportsFailover, supportsLoadBalancing);
         connectorManager.unregisterConnector(connectorObjectName.getCanonicalName());
         
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

   public void setJNDIBindings(Element e) throws Exception
   {
      jndiBindings = new JNDIBindings(e);
   }

   public Element getJNDIBindings()
   {
      if (jndiBindings == null)
      {
         return null;
      }
      return jndiBindings.getDelegate();
   }

   public void setServerPeer(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot change the value of associated " +
                  "server ObjectName after initialization!");
         return;
      }

      serverPeerObjectName = on;
   }

   public ObjectName getServerPeer()
   {
      return serverPeerObjectName;
   }
   
   public void setConnector(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot change the value of associated " +
                  "connector ObjectName after initialization!");
         return;
      }

      connectorObjectName = on;
   }

   public ObjectName getConnector()
   {
      return connectorObjectName;
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
      return this.dupsOKBatchSize;
   }

    public boolean isStrictTck()
    {
        return strictTck;
    }

    public void setStrictTck(boolean strictTck)
    {
        this.strictTck = strictTck;
    }

    // JMX managed operations -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
