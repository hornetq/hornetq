/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import javax.management.ObjectName;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ConnectorManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.remoting.InvokerLocator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * A deployable JBoss Messaging connection factory.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   protected String clientID;
   protected JNDIBindings jndiBindings;
   protected int prefetchSize = 150;
   protected boolean clustered;
   protected LoadBalancingFactory loadBalancingFactory;
   
   protected int defaultTempQueueFullSize = 75000;
   protected int defaultTempQueuePageSize = 2000;
   protected int defaultTempQueueDownCacheSize = 2000;

   protected ObjectName serverPeerObjectName;
   protected ConnectionFactoryManager connectionFactoryManager;
   
   protected ConnectorManager connectorManager;
   protected ConnectionManager connectionManager;
      
   protected ObjectName connectorObjectName;

   protected boolean started;

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
                                      locatorURI, enablePing, prefetchSize,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,
                                      defaultTempQueueDownCacheSize, clustered,
                                      loadBalancingFactory);
      
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
            unregisterConnectionFactory(getServiceName().getCanonicalName(), clustered);
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
   
   public boolean isClustered()
   {
      return clustered;
   }
   
   public void setClustered(boolean clustered)
   {
      if (started)
      {
         log.warn("Clustered can only be changed when connection factory is stopped");
         return;
      }
      this.clustered = clustered;
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
      
      Class clz = Class.forName(factoryName);
      
      loadBalancingFactory = (LoadBalancingFactory)clz.newInstance();
   }

   // JMX managed operations -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "[" + jndiBindings.toString() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
