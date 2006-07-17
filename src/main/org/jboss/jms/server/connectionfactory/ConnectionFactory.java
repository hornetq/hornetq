/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import org.jboss.system.ServiceMBeanSupport;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ConnectorManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.w3c.dom.Element;

import javax.management.ObjectName;

/**
 * A deployable JBoss Messaging connection factory.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactory extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected String clientID;
   protected int connectionFactoryID;
   protected JNDIBindings jndiBindings;
   protected int prefetchSize = 150;

   protected ObjectName serverPeerObjectName;
   protected ConnectionFactoryManager connectionFactoryManager;
   
   protected ConnectorManager connectorManager;
   protected ConnectionManager connectionManager;
      
   protected ObjectName connectorObjectName;

   protected boolean started;

   // Constructors --------------------------------------------------

   public ConnectionFactory()
   {
      this(null);
   }

   public ConnectionFactory(String clientID)
   {
      this.clientID = clientID;
   }

   // ServiceMBeanSupport overrides ---------------------------------

   public synchronized void startService() throws Exception
   {
      started = true;
      
      if (connectorObjectName == null)
      {
         throw new IllegalArgumentException("A Connector must be specified for each Connection Factory");
      }
      
      if (serverPeerObjectName == null)
      {
         throw new IllegalArgumentException("ServerPeer must be specified for each Connection Factory");
      }

      String locatorURI = (String)server.getAttribute(connectorObjectName, "InvokerLocator");
      ServerPeer serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");
      
      connectionFactoryManager = serverPeer.getConnectionFactoryManager();
      connectorManager = serverPeer.getConnectorManager();
      connectionManager = serverPeer.getConnectionManager();
      
      long leasePeriod = ((Long)server.getAttribute(connectorObjectName, "LeasePeriod")).longValue();
      
      // if leasePeriod <= 0, disable pinging altogether
      
      int refCount = connectorManager.registerConnector(connectorObjectName.getCanonicalName());
      
      boolean enablePing = leasePeriod > 0;
      
      if (refCount == 1 && enablePing)
      {
         // install the connection listener that listens for failed connections            
         server.invoke(connectorObjectName, "addConnectionListener",
               new Object[] {connectionManager},
               new String[] {"org.jboss.remoting.ConnectionListener"});                     
      }
      
      connectionFactoryID = connectionFactoryManager.
         registerConnectionFactory(clientID, jndiBindings, locatorURI, enablePing, prefetchSize);
              
      if (enablePing)
      {
         log.info("Connector has leasing enabled, lease period " + leasePeriod + " milliseconds");
      }
      else
      {
         log.info("Connector has lease disabled");
      }
      
      log.info(this + " deployed");
   }

   public synchronized void stopService() throws Exception
   {
      started = false;
      
      connectionFactoryManager.unregisterConnectionFactory(connectionFactoryID);
      
      connectorManager.unregisterConnector(connectorObjectName.getCanonicalName());
      
      log.info(this + " undeployed");
   }

   // JMX managed attributes ----------------------------------------
   
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

   // JMX managed operations ----------------------------------------

   // Public --------------------------------------------------------

   public String toString()
   {
      return "[" + jndiBindings.toString() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
