/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import org.jboss.system.ServiceMBeanSupport;
import org.jboss.jms.server.DestinationManager;
import org.w3c.dom.Element;

import javax.management.ObjectName;

/**
 * The base of a  JBoss Messaging destination service. Both deployed or programatically created
 * destinations will eventually get one of these.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DestinationServiceSupport extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ObjectName serverPeerObjectName;
   protected DestinationManager dm;
   protected Element securityConfig;

   protected String name;
   protected String jndiName;

   protected boolean started = false;
   private boolean createdProgrammatically = false;

   // Constructors --------------------------------------------------

   public DestinationServiceSupport(boolean createdProgrammatically)
   {
      this.createdProgrammatically = createdProgrammatically;
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

      dm = (DestinationManager)server.getAttribute(serverPeerObjectName, "Instance");

      jndiName = dm.registerDestination(isQueue(), name, jndiName, securityConfig);

      if (securityConfig == null)
      {
         // refresh my security config to be identical with server's default
         securityConfig = dm.getDefaultSecurityConfiguration();
      }

      log.info(this + " started");
   }

   public void stopService() throws Exception
   {
      dm.unregisterDestination(isQueue(), name);
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

   public void setSecurityConfig(Element securityConfig) throws Exception
   {
      // push security update to the server
      if (dm != null)
      {
         dm.setSecurityConfiguration(isQueue(), name, securityConfig);
      }

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

   public boolean isCreatedProgrammatically()
   {
      return createdProgrammatically;
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
      int idx = -1;
      if (jndiName != null)
      {
         jndiName.lastIndexOf('/');
      }
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
