/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import java.util.List;

import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.messaging.util.Logger;

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


   // JMX managed attributes -----------------------------------------------------------------------


   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

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

   public boolean isSupportsFailover()
   {
      return supportsFailover;
   }
   
   public void setSupportsFailover(boolean supportsFailover)
   {
      this.supportsFailover = supportsFailover;
   }
   
   public boolean isSupportsLoadBalancing()
   {
      return supportsLoadBalancing;
   }
   
   public void setSupportsLoadBalancing(boolean supportsLoadBalancing)
   {
      this.supportsLoadBalancing = supportsLoadBalancing;
   }

   public String getLoadBalancingFactory()
   {
      return loadBalancingFactory.getClass().getName();
   }

   public void setLoadBalancingFactory(String factoryName) throws Exception
   {
      
      //We don't use Class.forName() since then it won't work with scoped deployments
      Class clz = Thread.currentThread().getContextClassLoader().loadClass(factoryName);
      
      loadBalancingFactory = (LoadBalancingFactory)clz.newInstance();
   }
   
   public void setDupsOKBatchSize(int size) throws Exception
   {
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
   	this.strictTck = strictTck;
   }
   
   public boolean isDisableRemotingChecks()
   {
   	return disableRemotingChecks;
   }
   
   public void setDisableRemotingChecks(boolean disable)
   {
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

   
   // Inner classes --------------------------------------------------------------------------------
}
