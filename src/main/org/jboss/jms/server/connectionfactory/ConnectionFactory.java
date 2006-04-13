/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.connectionfactory;

import org.jboss.system.ServiceMBeanSupport;
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

   protected ObjectName serverPeerObjectName;
   protected ConnectionFactoryManager connectionFactoryManager;
   
   protected ConnectorManager connectorManager;
      
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
      
      long leasePeriod = ((Long)server.getAttribute(connectorObjectName, "LeasePeriod")).longValue();
      
      //If leasePeriod <= 0, disable pinging altogether
      
      boolean enablePing = leasePeriod > 0;
      
      connectorManager.registerConnector(connectorObjectName, enablePing);
      
      connectionFactoryID = connectionFactoryManager.registerConnectionFactory(clientID, jndiBindings, locatorURI, enablePing);
              
      if (enablePing)
      {
         log.info("Connector has leasing enabled, lease period=" + leasePeriod);
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
      
      connectorManager.unregisterConnector(connectorObjectName);
      
      log.info(this + " undeployed");
   }

   // JMX managed attributes ----------------------------------------

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
