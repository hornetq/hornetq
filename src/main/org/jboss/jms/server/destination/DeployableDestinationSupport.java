/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

import javax.management.ObjectName;

/**
 * The base of a deployable JBoss Messaging destination.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DeployableDestinationSupport extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // A destination interacts with the server peer via detyped JMX invocations. This decouples the
   // destination service from the server service and allows each of them to be hot redeployed
   // independently of each other.
   // TODO - test for that
   protected ObjectName serverPeerObjectName;

   protected Element securityConfig;

   // the destination name
   protected String name;

   protected String jndiName;

   protected boolean started = false;

   private final String createDestinationMethodName, destroyDestinationMethodName;

   // Constructors --------------------------------------------------

   protected DeployableDestinationSupport()
   {
      createDestinationMethodName = "create" + (isQueue() ? "Queue" : "Topic");
      destroyDestinationMethodName = "destroy" + (isQueue() ? "Queue" : "Topic");
   }

   // ServiceMBeanSupport overrides ---------------------------------

   public synchronized void startService() throws Exception
   {
      started = true;

      if (serviceName != null)
      {
         name = serviceName.getKeyProperty("name");
      }

      if (name == null || name.length() == 0)
      {
         throw new IllegalStateException( "The " + (isQueue() ? "queue" : "topic") +
                                          " name was not properly set in the service's ObjectName");
      }

      jndiName = (String)server.invoke(serverPeerObjectName, createDestinationMethodName,
                                       new Object[] {name, jndiName},
                                       new String[] {"java.lang.String", "java.lang.String"});

      if (securityConfig != null)
      {
         server.invoke(serverPeerObjectName, "configureSecurityForDestination",
                       new Object[] {name, securityConfig},
                       new String[] {"java.lang.String", "org.w3c.dom.Element"});
      }
      else
      {
         // relying on the server's default, expose it in the management interface for convenience
         Element defConfig = (Element)server.getAttribute(serverPeerObjectName,
                                                          "DefaultSecurityConfig");

         setSecurityConfig(defConfig);
      }

      log.info(this + " started");
   }

   public void stopService() throws Exception
   {
      server.invoke(serverPeerObjectName, destroyDestinationMethodName,
                    new Object[] {name},
                    new String[] {"java.lang.String"});

      started = false;

      log.info(this + " stopped");
   }

   // JMX managed attributes ----------------------------------------

   public String getJNDIName()
   {
      return jndiName;
   }

   public void setJNDIName(String jndiName)
   {
      if (started)
      {
         log.warn("Cannot change the value of the JNDI name after initialization!");
         return;
      }

      this.jndiName = jndiName;
   }

   public void setServerPeer(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot change the value of associated " +
                  "server's ObjectName after initialization!");
         return;
      }

      serverPeerObjectName = on;
   }

   public ObjectName getServerPeer()
   {
      return serverPeerObjectName;
   }

   public void setSecurityConfig(Element securityConfig)
   {
      this.securityConfig = securityConfig;
   }

   public Element getSecurityConfig()
   {
      return securityConfig;
   }

   public String getName()
   {
      return name;
   }

   // JMX managed operations ----------------------------------------

   // TODO implement the following:

//   void removeAllMessages() throws Exception;
//
//   public MessageCounter[] getMessageCounter();
//
//   public MessageStatistics[] getMessageStatistics() throws Exception;
//
//   public String listMessageCounter();
//
//   public void resetMessageCounter();
//
//   public String listMessageCounterHistory();
//
//   public void resetMessageCounterHistory();
//
//   public void setMessageCounterHistoryDayLimit( int days );
//
//   public int getMessageCounterHistoryDayLimit();
//
//   public int getMaxDepth();
//
//   public void setMaxDepth(int depth);
//
//   public boolean getInMemory();
//
//   public void setInMemory(boolean mode);
//
//   public int getRedeliveryLimit();
//
//   public void setRedeliveryLimit(int limit);
//
//   public long getRedeliveryDelay();
//
//   public void setRedeliveryDelay(long rDelay);
//
//   public Class getReceiversImpl();
//
//   public void setReceiversImpl(Class receivers);
//
//   public int getRecoveryRetries();
//
//   public void setRecoveryRetries(int retries);

   // Public --------------------------------------------------------

   public String toString()
   {
      String nameFromJNDI = jndiName;
      int idx = jndiName.lastIndexOf('/');
      if (idx != -1)
      {
         nameFromJNDI = jndiName.substring(idx + 1);
      }
      StringBuffer sb = new StringBuffer();
      if (isQueue())
      {
         sb.append("Queue");
      }
      else
      {
         sb.append("Topic");
      }
      sb.append('[');
      if (name.equals(nameFromJNDI))
      {
         sb.append(jndiName);
      }
      else
      {
         sb.append(jndiName).append(", name=").append(name);
      }
      sb.append(']');
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract boolean isQueue();

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
