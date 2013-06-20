/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.ra.inflow;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;

import org.hornetq.ra.ConnectionFactoryProperties;
import org.hornetq.ra.HornetQRALogger;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.HornetQRaUtils;

/**
 * The activation spec
 * These properties are set on the MDB ActivactionProperties
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class HornetQActivationSpec extends ConnectionFactoryProperties implements ActivationSpec, Serializable
{
   private static final long serialVersionUID = -7997041053897964654L;

   private static final int DEFAULT_MAX_SESSION = 15;

   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   public String strConnectorClassName;

   public String strConnectionParameters;

   /** The resource adapter */
   private HornetQResourceAdapter ra;

   /** The connection factory lookup */
   private String connectionFactoryLookup;

   /** The destination */
   private String destination;

   /** The destination type */
   private String destinationType;

   /** The message selector */
   private String messageSelector;

   /** The acknowledgement mode */
   private int acknowledgeMode;

   /** The subscription durability */
   private boolean subscriptionDurability;

   /** The subscription name */
   private String subscriptionName;

   /**
    * If this is true, a durable subscription could be shared by multiple MDB instances
    */
   private boolean shareSubscriptions;

   /** The user */
   private String user;

   /** The password */
   private String password;

   /** The maximum number of sessions */
   private Integer maxSession;

   /** Transaction timeout */
   private Integer transactionTimeout;

   private Boolean useJNDI = true;

   private String jndiParams = null;

   private Hashtable parsedJndiParams;

   /* use local tx instead of XA*/
   private Boolean localTx;

   // undefined by default, default is specified at the RA level in HornetQRAProperties
   private Integer setupAttempts;

   // undefined by default, default is specified at the RA level in HornetQRAProperties
   private Long setupInterval;

   /**
    * Constructor
    */
   public HornetQActivationSpec()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor()");
      }

      ra = null;
      destination = null;
      destinationType = null;
      messageSelector = null;
      acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
      subscriptionDurability = false;
      subscriptionName = null;
      user = null;
      password = null;
      maxSession = DEFAULT_MAX_SESSION;
      transactionTimeout = 0;
   }

   /**
    * Get the resource adapter
    * @return The resource adapter
    */
   public ResourceAdapter getResourceAdapter()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getResourceAdapter()");
      }

      return ra;
   }

   /**
    * @return the useJNDI
    */
   public boolean isUseJNDI()
   {
      if(useJNDI == null)
      {
         return ra.isUseJNDI();
      }
      return useJNDI;
   }

   /**
    * @param value the useJNDI to set
    */
   public void setUseJNDI(final boolean value)
   {
      useJNDI = value;
   }

   /**
    *
    * @return return the jndi params to use
    */
   public String getJndiParams()
   {
      if(jndiParams == null)
      {
         return ra.getJndiParams();
      }
      return jndiParams;
   }

   public void setJndiParams(String jndiParams)
   {
      this.jndiParams = jndiParams;
      parsedJndiParams = HornetQRaUtils.parseHashtableConfig(jndiParams);
   }

   public Hashtable<?,?> getParsedJndiParams()
   {
      if(parsedJndiParams == null)
      {
         return ra.getParsedJndiParams();
      }
      return parsedJndiParams;
   }

   /**
    * Set the resource adapter
    * @param ra The resource adapter
    * @exception ResourceException Thrown if incorrect resource adapter
    */
   public void setResourceAdapter(final ResourceAdapter ra) throws ResourceException
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setResourceAdapter(" + ra + ")");
      }

      if (ra == null || !(ra instanceof HornetQResourceAdapter))
      {
         throw new ResourceException("Resource adapter is " + ra);
      }

      this.ra = (HornetQResourceAdapter)ra;
   }

   /**
    * Get the connection factory lookup
    * @return The value
    */
   public String getConnectionFactoryLookup()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getConnectionFactoryLookup() ->" + connectionFactoryLookup);
      }

      return connectionFactoryLookup;
   }

   /**
    * Set the connection factory lookup
    * @param value The value
    */
   public void setConnectionFactoryLookup(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setConnectionFactoryLookup(" + value + ")");
      }

      connectionFactoryLookup = value;
   }

   /**
    * Get the destination
    * @return The value
    */
   public String getDestination()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getDestination()");
      }

      return destination;
   }

   /**
    * Set the destination
    * @param value The value
    */
   public void setDestination(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setDestination(" + value + ")");
      }

      destination = value;
   }

   /**
    * Get the destination lookup
    * @return The value
    */
   public String getDestinationLookup()
   {
      return getDestination();
   }

   /**
    * Set the destination
    * @param value The value
    */
   public void setDestinationLookup(final String value)
   {
      setDestination(value);
      setUseJNDI(true);
   }

   /**
    * Get the destination type
    * @return The value
    */
   public String getDestinationType()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getDestinationType()");
      }

      return destinationType;
   }

   /**
    * Set the destination type
    * @param value The value
    */
   public void setDestinationType(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setDestinationType(" + value + ")");
      }

      destinationType = value;
   }

   /**
    * Get the message selector
    * @return The value
    */
   public String getMessageSelector()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getMessageSelector()");
      }

      return messageSelector;
   }

   /**
    * Set the message selector
    * @param value The value
    */
   public void setMessageSelector(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setMessageSelector(" + value + ")");
      }

      messageSelector = value;
   }

   /**
    * Get the acknowledge mode
    * @return The value
    */
   public String getAcknowledgeMode()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getAcknowledgeMode()");
      }

      if (Session.DUPS_OK_ACKNOWLEDGE == acknowledgeMode)
      {
         return "Dups-ok-acknowledge";
      }
      else
      {
         return "Auto-acknowledge";
      }
   }

   /**
    * Set the acknowledge mode
    * @param value The value
    */
   public void setAcknowledgeMode(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setAcknowledgeMode(" + value + ")");
      }

      if ("DUPS_OK_ACKNOWLEDGE".equalsIgnoreCase(value) || "Dups-ok-acknowledge".equalsIgnoreCase(value))
      {
         acknowledgeMode = Session.DUPS_OK_ACKNOWLEDGE;
      }
      else if ("AUTO_ACKNOWLEDGE".equalsIgnoreCase(value) || "Auto-acknowledge".equalsIgnoreCase(value))
      {
         acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
      }
      else
      {
         throw new IllegalArgumentException("Unsupported acknowledgement mode " + value);
      }
   }

   /**
    * @return the acknowledgement mode
    */
   public int getAcknowledgeModeInt()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getAcknowledgeMode()");
      }

      return acknowledgeMode;
   }

   /**
    * Get the subscription durability
    * @return The value
    */
   public String getSubscriptionDurability()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getSubscriptionDurability()");
      }

      if (subscriptionDurability)
      {
         return "Durable";
      }
      else
      {
         return "NonDurable";
      }
   }

   /**
    * Set the subscription durability
    * @param value The value
    */
   public void setSubscriptionDurability(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setSubscriptionDurability(" + value + ")");
      }

      subscriptionDurability = "Durable".equals(value);
   }

   /**
    * Get the status of subscription durability
    * @return The value
    */
   public boolean isSubscriptionDurable()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("isSubscriptionDurable()");
      }

      return subscriptionDurability;
   }

   /**
    * Get the subscription name
    * @return The value
    */
   public String getSubscriptionName()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getSubscriptionName()");
      }

      return subscriptionName;
   }

   /**
    * Set the subscription name
    * @param value The value
    */
   public void setSubscriptionName(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setSubscriptionName(" + value + ")");
      }

      subscriptionName = value;
   }



   /**
    * @return the shareDurableSubscriptions
    */
   public boolean isShareSubscriptions()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("isShareSubscriptions() = " + shareSubscriptions);
      }

      return shareSubscriptions;
   }

   /**
    * @param shareSubscriptions the shareDurableSubscriptions to set
    */
   public void setShareSubscriptions(boolean shareSubscriptions)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setShareSubscriptions(" +  shareSubscriptions + ")");
      }

      this.shareSubscriptions = shareSubscriptions;
   }

   /**
    * Get the user
    * @return The value
    */
   public String getUser()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getUser()");
      }

      if (user == null)
      {
         return ra.getUserName();
      }
      else
      {
         return user;
      }
   }

   /**
    * Set the user
    * @param value The value
    */
   public void setUser(final String value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setUser(" + value + ")");
      }

      user = value;
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getPassword()");
      }

      if (password == null)
      {
         return ra.getPassword();
      }
      else
      {
         return password;
      }
   }

   public String getOwnPassword()
   {
      return password;
   }

   /**
    * Set the password
    * @param value The value
    */
   public void setPassword(final String value) throws Exception
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setPassword(" + value + ")");
      }

      password = value;
   }

   /**
    * Get the number of max session
    * @return The value
    */
   public Integer getMaxSession()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getMaxSession()");
      }

      if (maxSession == null)
      {
         return DEFAULT_MAX_SESSION;
      }

      return maxSession;
   }

   /**
    * Set the number of max session
    * @param value The value
    */
   public void setMaxSession(final Integer value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setMaxSession(" + value + ")");
      }

      maxSession = value;
   }

   /**
    * Get the transaction timeout
    * @return The value
    */
   public Integer getTransactionTimeout()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getTransactionTimeout()");
      }

      return transactionTimeout;
   }

   /**
    * Set the transaction timeout
    * @param value The value
    */
   public void setTransactionTimeout(final Integer value)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setTransactionTimeout(" + value + ")");
      }

      transactionTimeout = value;
   }

   public Boolean isUseLocalTx()
   {
      if (localTx == null)
      {
         return ra.getUseLocalTx();
      }
      else
      {
         return localTx;
      }
   }

   public void setUseLocalTx(final Boolean localTx)
   {
      this.localTx = localTx;
   }

   public int getSetupAttempts()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getSetupAttempts()");
      }

      if (setupAttempts == null)
      {
         return ra.getSetupAttempts();
      }
      else
      {
         return setupAttempts;
      }
   }

   public void setSetupAttempts(int setupAttempts)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setSetupAttempts(" + setupAttempts + ")");
      }

      this.setupAttempts = setupAttempts;
   }

   public long getSetupInterval()
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("getSetupInterval()");
      }

      if (setupInterval == null)
      {
         return ra.getSetupInterval();
      }
      else
      {
         return setupInterval;
      }
   }

   public void setSetupInterval(long setupInterval)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setSetupInterval(" + setupInterval + ")");
      }

      this.setupInterval = setupInterval;
   }

   /**
    * Validate
    * @exception InvalidPropertyException Thrown if a validation exception occurs
    */
   public void validate() throws InvalidPropertyException
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("validate()");
      }

      if (destination == null || destination.trim().equals(""))
      {
         throw new InvalidPropertyException("Destination is mandatory");
      }
   }

   public String getConnectorClassName()
   {
      return strConnectorClassName;
   }

   public void setConnectorClassName(final String connectorClassName)
   {
      if (HornetQActivationSpec.trace)
      {
         HornetQRALogger.LOGGER.trace("setConnectorClassName(" + connectorClassName + ")");
      }

      strConnectorClassName = connectorClassName;

      setParsedConnectorClassNames(HornetQRaUtils.parseConnectorConnectorConfig(connectorClassName));
   }

   /**
    * @return the connectionParameters
    */
   public String getConnectionParameters()
   {
      return strConnectionParameters;
   }

   public void setConnectionParameters(final String configuration)
   {
      strConnectionParameters = configuration;
      setParsedConnectionParameters(HornetQRaUtils.parseConfig(configuration));
   }

   /**
    * Get a string representation
    * @return The value
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append(HornetQActivationSpec.class.getName()).append('(');
      buffer.append("ra=").append(ra);
      if (messageSelector != null)
      {
         buffer.append(" connectionFactoryLookup=").append(connectionFactoryLookup);
      }
      buffer.append(" destination=").append(destination);
      buffer.append(" destinationType=").append(destinationType);
      if (messageSelector != null)
      {
         buffer.append(" selector=").append(messageSelector);
      }
      buffer.append(" ack=").append(getAcknowledgeMode());
      buffer.append(" durable=").append(subscriptionDurability);
      buffer.append(" clientID=").append(getClientID());
      if (subscriptionName != null)
      {
         buffer.append(" subscription=").append(subscriptionName);
      }
      buffer.append(" user=").append(user);
      if (password != null)
      {
         buffer.append(" password=").append("****");
      }
      buffer.append(" maxSession=").append(maxSession);
      buffer.append(')');
      return buffer.toString();
   }

   // here for backwards compatibilty
   public void setUseDLQ(final boolean b)
   {
   }

   public void setDLQJNDIName(final String name)
   {
   }

   public void setDLQHandler(final String handler)
   {
   }

   public void setDLQMaxResent(final int maxResent)
   {
   }

   public void setProviderAdapterJNDI(final String jndi)
   {
   }

   /**
    * @param keepAlive the keepAlive to set
    */
   public void setKeepAlive(boolean keepAlive)
   {
   }

    /**
    * @param keepAliveMillis the keepAliveMillis to set
    */
   public void setKeepAliveMillis(long keepAliveMillis)
   {
   }


   public void setReconnectInterval(long interval)
   {
   }

   public void setMinSession(final Integer value)
   {
   }

   public void setMaxMessages(final Integer value)
   {
   }


}
