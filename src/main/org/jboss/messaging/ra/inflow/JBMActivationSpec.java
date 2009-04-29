/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.ra.inflow;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.ra.JBMResourceAdapter;
import org.jboss.messaging.ra.Util;

import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;

/**
 * The activation spec
 * These properties are set on the MDB ActivactionProperties
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version $Revision: $
 */
public class JBMActivationSpec implements ActivationSpec
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMActivationSpec.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();
   
   /** The transport config, changing the default configured from the RA */
   private Map<String, Object> connectionParameters = new HashMap<String, Object>();
   
   public String strConnectionParameters;

   /** The transport type, changing the default configured from the RA */
   private String connectorClassName;

   /** The resource adapter */
   private JBMResourceAdapter ra;

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

   /** The client id */
   private String clientId;

   /** The user */
   private String user;

   /** The password */
   private String password;

   /** The maximum number of messages */
   private Integer maxMessages;

   /** The minimum number of sessions */
   private Integer minSession;

   /** The maximum number of sessions */
   private Integer maxSession;

   /** The keep alive time for sessions */
   private Long keepAlive;

   /** Is the session transacted */
   private Boolean sessionTransacted;

   /** Unspecified redelivery */
   private Boolean redeliverUnspecified;
   
   /** Transaction timeout */
   private Integer transactionTimeout;
   
   /** Is same RM override */
   private Boolean isSameRMOverrideValue;
   
   private boolean useJNDI = true;
   
   /** Force clear on shutdown */
   private Boolean forceClearOnShutdown;
   
   /** Force clear internal */
   private Long forceClearOnShutdownInterval;
   
   /** Force clear attempts */
   private Integer forceClearAttempts;
   
   /**
    * Constructor
    */
   public JBMActivationSpec()
   {
      if (trace)
         log.trace("constructor()");

      ra = null;
      destination = null;
      destinationType = null;
      messageSelector = null;
      acknowledgeMode = Session.SESSION_TRANSACTED;
      subscriptionDurability = false;
      subscriptionName = null;
      clientId = null;
      user = null;
      password = null;
      maxMessages = Integer.valueOf(1);
      minSession = Integer.valueOf(1);
      maxSession = Integer.valueOf(15);
      keepAlive = Long.valueOf(60000);
      sessionTransacted = Boolean.TRUE;
      redeliverUnspecified = Boolean.TRUE;
      transactionTimeout = Integer.valueOf(0);
      isSameRMOverrideValue = null;
      forceClearOnShutdown = Boolean.FALSE;
      forceClearOnShutdownInterval = Long.valueOf(1000);
      forceClearAttempts = Integer.valueOf(0);
   }

   /**
    * Get the resource adapter
    * @return The resource adapter
    */
   public ResourceAdapter getResourceAdapter()
   {
      if (trace)
         log.trace("getResourceAdapter()");

      return ra;
   }

   
   
   /**
    * @return the useJNDI
    */
   public boolean isUseJNDI()
   {
      return useJNDI;
   }

   /**
    * @param useJNDI the useJNDI to set
    */
   public void setUseJNDI(boolean value)
   {
      this.useJNDI = value;
   }

   /**
    * Set the resource adapter
    * @param ra The resource adapter
    * @exception ResourceException Thrown if incorrect resource adapter
    */
   public void setResourceAdapter(ResourceAdapter ra) throws ResourceException
   {
      if (trace)
         log.trace("setResourceAdapter(" + ra + ")");

      if (ra == null || !(ra instanceof JBMResourceAdapter)) {
         throw new ResourceException("Resource adapter is " + ra);
      }

      this.ra = (JBMResourceAdapter)ra;
   }

   /**
    * Get the destination
    * @return The value
    */
   public String getDestination()
   {
      if (trace)
         log.trace("getDestination()");

      return destination;
   }

   /**
    * Set the destination
    * @param value The value
    */
   public void setDestination(String value)
   {
      if (trace)
         log.trace("setDestination(" + value + ")");

      this.destination = value;
   }

   /**
    * Get the destination type
    * @return The value
    */
   public String getDestinationType()
   {
      if (trace)
         log.trace("getDestinationType()");

      return destinationType;
   }

   /**
    * Set the destination type
    * @param value The value
    */
   public void setDestinationType(String value)
   {
      if (trace)
         log.trace("setDestinationType(" + value + ")");

      this.destinationType = value;
   }

   /**
    * Get the message selector
    * @return The value
    */
   public String getMessageSelector()
   {
      if (trace)
         log.trace("getMessageSelector()");

      return messageSelector;
   }

   /**
    * Set the message selector
    * @param value The value
    */
   public void setMessageSelector(String value)
   {
      if (trace)
         log.trace("setMessageSelector(" + value + ")");

      this.messageSelector = value;
   }

   /**
    * Get the acknowledge mode
    * @return The value
    */
   public String getAcknowledgeMode()
   {
      if (trace)
         log.trace("getAcknowledgeMode()");

      if (sessionTransacted.booleanValue())
         return "Transacted";
      else if (Session.DUPS_OK_ACKNOWLEDGE == acknowledgeMode)
         return "Dups-ok-acknowledge";
      else
         return "Auto-acknowledge";
   }

   /**
    * Set the acknowledge mode
    * @param value The value
    */
   public void setAcknowledgeMode(String value)
   {
      if (trace)
         log.trace("setAcknowledgeMode(" + value + ")");

      if ("DUPS_OK_ACKNOWLEDGE".equals(value) || "Dups-ok-acknowledge".equals(value))
         this.acknowledgeMode = Session.DUPS_OK_ACKNOWLEDGE;
      else if ("AUTO_ACKNOWLEDGE".equals(value) || "Auto-acknowledge".equals(value))
         this.acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
      else if ("SESSION_TRANSACTED".equals(value))
         this.acknowledgeMode = Session.SESSION_TRANSACTED;
      else
         throw new IllegalArgumentException("Unsupported acknowledgement mode " + value);
   }

   /**
    * @return the acknowledgement mode
    */
   public int getAcknowledgeModeInt()
   {
      if (trace)
         log.trace("getAcknowledgeMode()");

      if (sessionTransacted.booleanValue())
         return Session.SESSION_TRANSACTED;

      return acknowledgeMode;
   }

   /**
    * Get the subscription durability
    * @return The value
    */
   public String getSubscriptionDurability()
   {
      if (trace)
         log.trace("getSubscriptionDurability()");

      if (subscriptionDurability)
         return "Durable";
      else
         return "NonDurable";
   }

   /**
    * Set the subscription durability
    * @param value The value
    */
   public void setSubscriptionDurability(String value)
   {
      if (trace)
         log.trace("setSubscriptionDurability(" + value + ")");

      this.subscriptionDurability = "Durable".equals(value);
   }

   /**
    * Get the status of subscription durability
    * @return The value
    */
   public boolean isSubscriptionDurable()
   {
      if (trace)
         log.trace("isSubscriptionDurable()");

      return subscriptionDurability;
   }

   /**
    * Get the subscription name
    * @return The value
    */
   public String getSubscriptionName()
   {
      if (trace)
         log.trace("getSubscriptionName()");

      return subscriptionName;
   }

   /**
    * Set the subscription name
    * @param value The value
    */
   public void setSubscriptionName(String value)
   {
      if (trace)
         log.trace("setSubscriptionName(" + value + ")");

      this.subscriptionName = value;
   }

   /**
    * Get the client id
    * @return The value
    */
   public String getClientId()
   {
      if (trace)
         log.trace("getClientId()");

      if (clientId == null)
      {
         return ra.getClientID();
      }
      return clientId;
   }

   /**
    * Set the client id
    * @param value The value
    */
   public void setClientId(String value)
   {
      if (trace)
         log.trace("setClientId(" + value + ")");

      this.clientId = value;
   }

   /**
    * Get the user
    * @return The value
    */
   public String getUser()
   {
      if (trace)
         log.trace("getUser()");
      
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
   public void setUser(String value)
   {
      if (trace)
         log.trace("setUser(" + value + ")");

      this.user = value;
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (trace)
         log.trace("getPassword()");
      
      if (password == null)
      {
         return ra.getPassword();
      }
      else
      {
         return password;
      }
   }

   /**
    * Set the password
    * @param value The value
    */
   public void setPassword(String value)
   {
      if (trace)
         log.trace("setPassword(" + value + ")");

      this.password = value;
   }

   /**
    * Get the numer of max messages
    * @return The value
    */
   public Integer getMaxMessages()
   {
      if (trace)
         log.trace("getMaxMessages()");

      return maxMessages;
   }

   /**
    * Set the numer of max messages
    * @param value The value
    */
   public void setMaxMessages(Integer value)
   {
      if (trace)
         log.trace("setMaxMessages(" + value + ")");

      this.maxMessages = value;
   }

   /**
    * Get the number of max messages
    * @return The value
    */
   public int getMaxMessagesInt()
   {
      if (trace)
         log.trace("getMaxMessagesInt()");

      if (maxMessages == null)
         return 0;

      return maxMessages.intValue();
   }

   /**
    * Get the number of min session
    * @return The value
    */
   public Integer getMinSession()
   {
      if (trace)
         log.trace("getMinSession()");

      return minSession;
   }

   /**
    * Set the number of min session
    * @param value The value
    */
   public void setMinSession(Integer value)
   {
      if (trace)
         log.trace("setMinSession(" + value + ")");

      this.minSession = value;
   }

   /**
    * Get the number of min session
    * @return The value
    */
   public int getMinSessionInt()
   {
      if (trace)
         log.trace("getMinSessionInt()");

      if (minSession == null)
         return 0;

      return minSession.intValue();
   }

   /**
    * Get the number of max session
    * @return The value
    */
   public Integer getMaxSession()
   {
      if (trace)
         log.trace("getMaxSession()");

      return maxSession;
   }

   /**
    * Set the number of max session
    * @param value The value
    */
   public void setMaxSession(Integer value)
   {
      if (trace)
         log.trace("setMaxSession(" + value + ")");

      maxSession = value;
   }

   /**
    * Get the number of max session
    * @return The value
    */
   public int getMaxSessionInt()
   {
      if (trace)
         log.trace("getMaxSessionInt()");

      if (maxSession == null)
         return 0;

      return maxSession.intValue();
   }

   /**
    * Get the keep alive
    * @return The value
    */
   public Long getKeepAlive()
   {
      if (trace)
         log.trace("getKeepAlive()");

      return keepAlive;
   }

   /**
    * Set the keep alive
    * @param value The value
    */
   public void setKeepAlive(Long value)
   {
      if (trace)
         log.trace("setKeepAlive(" + value + ")");

      this.keepAlive = value;
   }

   /**
    * Get the keep alive
    * @return The value
    */
   public long getKeepAliveLong()
   {
      if (trace)
         log.trace("getKeepAliveLong()");

      if (keepAlive == null)
         return 0;

      return keepAlive.longValue();
   }

   /**
    * Get the session transacted
    * @return The value
    */
   public Boolean getSessionTransacted()
   {
      if (trace)
         log.trace("getSessionTransacted()");

      return sessionTransacted;
   }

   /**
    * Set the session transacted
    * @param value The value
    */
   public void setSessionTransacted(Boolean value)
   {
      if (trace)
         log.trace("setTransactionTimeout(" + value + ")");

      this.sessionTransacted = value;
   }

   /**
    * Get the session transaced
    * @return THe value
    */
   public boolean isSessionTransacted()
   {
      if (trace)
         log.trace("isSessionTransacted()");

      if (sessionTransacted == null)
         return false;

      return sessionTransacted.booleanValue();
   }
   /**
    * Get the redeliver upspecified
    * @return The value
    */
   public Boolean getRedeliverUnspecified()
   {
      if (trace)
         log.trace("getRedeliverUnspecified()");

      return redeliverUnspecified;
   }

   /**
    * Set the redeliver unspecified
    * @param value The value
    */
   public void setRedeliverUnspecified(Boolean value)
   {
      if (trace)
         log.trace("setRedeliverUnspecified(" + value + ")");

      this.redeliverUnspecified = value;
   }
   
   /**
    * Get the transaction timeout
    * @return The value
    */
   public Integer getTransactionTimeout()
   {
      if (trace)
         log.trace("getTransactionTimeout()");

      return transactionTimeout;
   }

   /**
    * Set the transaction timeout
    * @param value The value
    */
   public void setTransactionTimeout(Integer value)
   {
      if (trace)
         log.trace("setTransactionTimeout(" + value + ")");

      transactionTimeout = value;
   }
   
   /**
    * Get the is same rm override
    * @return The value
    */
   public Boolean getIsSameRMOverrideValue()
   {
      if (trace)
         log.trace("getIsSameRMOverrideValue()");

      return isSameRMOverrideValue;
   }

   /**
    * Set the is same RM override
    * @param value The value
    */
   public void setIsSameRMOverrideValue(Boolean value)
   {
      if (trace)
         log.trace("setIsSameRMOverrideValue(" + value + ")");

      isSameRMOverrideValue = value;
   }

   /**
    * Get force clear on shutdown
    * @return The value
    */
   public Boolean getForceClearOnShutdown()
   {
      if (trace)
         log.trace("getForceClearOnShutdown()");

      return forceClearOnShutdown;
   }

   /**
    * Set the force clear on shutdown
    * @param value The value
    */
   public void setForceClearOnShutdown(Boolean value)
   {
      if (trace)
         log.trace("setForceClearOnShutdown(" + value + ")");

      this.forceClearOnShutdown = value;
   }   
   
   /**
    * Get force clear on shutdown
    * @return The value
    */
   public boolean isForceClearOnShutdown()
   {
      if (trace)
         log.trace("isForceClearOnShutdown()");

      if (forceClearOnShutdown == null)
         return false;

      return forceClearOnShutdown.booleanValue();
   }

   /**
    * Get force clear on shutdown interval
    * @return The value
    */
   public Long getForceClearOnShutdownInterval()
   {
      if (trace)
         log.trace("getForceClearOnShutdownInterval()");

      return forceClearOnShutdownInterval;
   }
   
   /**
    * Set the force clear on shutdown interval
    * @param value The value
    */
   public void setForceClearOnShutdownInterval(Long value)
   {
      if (trace)
         log.trace("setForceClearOnShutdownInterval(" + value + ")");

      forceClearOnShutdownInterval = value;
   }
   
   /**
    * Get force clear attempts
    * @return The value
    */
   public Integer getForceClearAttempts()
   {
      if (trace)
         log.trace("getForceClearAttempts()");

      return forceClearAttempts;
   }
   
   /**
    * Set the force clear attempts
    * @param value The value
    */
   public void setForceClearAttempts(Integer value)
   {
      if (trace)
         log.trace("setForceClearAttempts(" + value + ")");

      forceClearAttempts = value;
   }

   /**
    * Validate
    * @exception InvalidPropertyException Thrown if a validation exception occurs
    */
   public void validate() throws InvalidPropertyException
   {
      if (trace)
         log.trace("validate()");

      if (destination == null || destination.trim().equals(""))
         throw new InvalidPropertyException("Destination is mandatory");
   }

   /**
    * @return the connectionParameters
    */
   public String getConnectionParameters()
   {
      return strConnectionParameters;
   }
   
   public Map<String, Object> getParsedConnectionParameters()
   {
      return connectionParameters;
   }

   public void setConnectionParameters(String configuration)
   {
      this.strConnectionParameters = configuration;
      this.connectionParameters = Util.parseConfig(configuration);
   }
   
   /**
    * @return the connectorClassName
    */
   public String getConnectorClassName()
   {
      return connectorClassName;
   }
   
   public void setConnectorClassName(String value)
   {
      this.connectorClassName = value;
   }

   /**
    * Get a string representation
    * @return The value
    */
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append(JBMActivationSpec.class.getName()).append('(');
      buffer.append("ra=").append(ra);
      buffer.append(" destination=").append(destination);
      buffer.append(" destinationType=").append(destinationType);
      if (messageSelector != null)
         buffer.append(" selector=").append(messageSelector);
      buffer.append(" tx=").append(sessionTransacted);
      if (sessionTransacted == false)
         buffer.append(" ack=").append(getAcknowledgeMode());
      buffer.append(" durable=").append(subscriptionDurability);
      if (clientId != null)
         buffer.append(" clientID=").append(clientId);
      if (subscriptionName != null)
         buffer.append(" subscription=").append(subscriptionName);
      buffer.append(" user=").append(user);
      if (password != null)
         buffer.append(" password=").append("****");
      buffer.append(" maxMessages=").append(maxMessages);
      buffer.append(" minSession=").append(minSession);
      buffer.append(" maxSession=").append(maxSession);
      buffer.append(" keepAlive=").append(keepAlive);
      buffer.append(')');
      return buffer.toString();
   }
   
   //here for backwards compatibilty
   public void setUseDLQ(boolean b)
   {
      
   }

   public void setDLQJNDIName(String name)
   {

   }

   public void setDLQMaxResent(int maxResent)
   {

   }

   public void setProviderAdapterJNDI(String jndi)
   {

   }
}
