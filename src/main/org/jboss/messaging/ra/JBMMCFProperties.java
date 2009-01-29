/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.ra;

import java.io.Serializable;
import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.messaging.core.logging.Logger;

/**
 * The MCF default properties - these are set in the ra.xml file
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMMCFProperties implements Serializable
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMCFProperties.class);
   
   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The queue type */
   private static final String QUEUE_TYPE = Queue.class.getName();

   /** The topic type */
   private static final String TOPIC_TYPE = Topic.class.getName();

   /** The discovery group name */
   private String discoveryGroupName;

   /** The discovery group port */
   private Integer discoveryGroupPort;

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /** The client ID */
   private String clientID;

   /** Use XA */
   private Boolean useXA;

   /** The connection type */
   private int type = JBMConnectionFactory.CONNECTION;

   /** Use tryLock */
   private Integer useTryLock;
   
   /**
    * Constructor
    */
   public JBMMCFProperties()
   {
      if (trace)
         log.trace("constructor()");

      discoveryGroupName = null;
      discoveryGroupPort = null;
      userName = null;
      password = null;
      clientID = null;
      useXA = null;
      useTryLock = null;
   }
   
   /**
    * Get the discovery group name
    * @return The value
    */
   public String getDiscoveryGroupName()
   {
      if (trace)
         log.trace("getDiscoveryGroupName()");

      return discoveryGroupName;
   }

   /**
    * Set the discovery group name
    * @param dgn The value
    */
   public void setDiscoveryGroupName(String dgn)
   {
      if (trace)
         log.trace("setDiscoveryGroupName(" + dgn + ")");

      discoveryGroupName = dgn;
   }

   /**
    * Get the discovery group port
    * @return The value
    */
   public Integer getDiscoveryGroupPort()
   {
      if (trace)
         log.trace("getDiscoveryGroupPort()");

      return discoveryGroupPort;
   }

   /**
    * Set the discovery group port
    * @param dgp The value
    */
   public void setDiscoveryGroupPort(Integer dgp)
   {
      if (trace)
         log.trace("setDiscoveryGroupPort(" + dgp + ")");

      discoveryGroupPort = dgp;
   }

   /**
    * Get the user name
    * @return The value
    */
   public String getUserName()
   {
      if (trace)
         log.trace("getUserName()");

      return userName;
   }

   /**
    * Set the user name
    * @param userName The value
    */
   public void setUserName(String userName)
   {
      if (trace)
         log.trace("setUserName(" + userName + ")");

      this.userName = userName;
   }
  
   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (trace)
         log.trace("getPassword()");

      return password;
   }

   /**
    * Set the password
    * @param password The value
    */
   public void setPassword(String password)
   {
      if (trace)
         log.trace("setPassword(****)");

      this.password = password;
   }
  
   /**
    * Get the client id
    * @return The value
    */
   public String getClientID()
   {
      if (trace)
         log.trace("getClientID()");

      return clientID;
   }

   /**
    * Set the client id
    * @param clientID The value
    */
   public void setClientID(String clientID)
   {
      if (trace)
         log.trace("setClientID(" + clientID + ")");

      this.clientID = clientID;
   }
  
   /**
    * Get the use XA flag
    * @return The value
    */
   public Boolean getUseXA()
   {
      if (trace)
         log.trace("getUseXA()");

      return useXA;
   }

   /**
    * Set the use XA flag
    * @param xa The value
    */
   public void setUseXA(Boolean xa)
   {
      if (trace)
         log.trace("setUseXA(" + xa + ")");

      this.useXA = xa;
   }

   /**
    * Use XA for communication
    * @return The value
    */
   public boolean isUseXA()
   {
      if (trace)
         log.trace("isUseXA()");

      if (useXA == null)
         return false;

      return useXA.booleanValue();
   }

   /**
    * Get the connection type
    * @return The type
    */
   public int getType()
   {
      if (trace)
         log.trace("getType()");

      return type;
   }

   /**
    * Set the default session type.
    * @param defaultType either javax.jms.Topic or javax.jms.Queue
    */
   public void setSessionDefaultType(String defaultType)
   {
      if (trace)
         log.trace("setSessionDefaultType(" + type + ")");

      if (defaultType.equals(QUEUE_TYPE))
         this.type = JBMConnectionFactory.QUEUE_CONNECTION;
      else if(defaultType.equals(TOPIC_TYPE))
         this.type = JBMConnectionFactory.TOPIC_CONNECTION;
      else
         this.type = JBMConnectionFactory.CONNECTION;
   }

   /**
    * Get the default session type.
    * @return The default session type
    */
   public String getSessionDefaultType()
   {
      if (trace)
         log.trace("getSessionDefaultType()");

      if (type == JBMConnectionFactory.CONNECTION)
         return "BOTH";
      else if (type == JBMConnectionFactory.QUEUE_CONNECTION)
         return TOPIC_TYPE;
      else
         return QUEUE_TYPE;
   }

   /**
    * Get the useTryLock.
    * @return the useTryLock.
    */
   public Integer getUseTryLock()
   {
      if (trace)
         log.trace("getUseTryLock()");

      return useTryLock;
   }
   
   /**
    * Set the useTryLock.
    * @param useTryLock the useTryLock.
    */
   public void setUseTryLock(Integer useTryLock)
   {
      if (trace)
         log.trace("setUseTryLock(" + useTryLock + ")");

      this.useTryLock = useTryLock;
   }
   
   /**
    * Indicates whether some other object is "equal to" this one.
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   public boolean equals(Object obj)
   {
      if (trace)
         log.trace("equals(" + obj + ")");

      if (obj == null) 
         return false;
    
      if (obj instanceof JBMMCFProperties)
      {
         JBMMCFProperties you = (JBMMCFProperties) obj;
         return (Util.compare(discoveryGroupName, you.getDiscoveryGroupName()) &&
                 Util.compare(discoveryGroupPort, you.getDiscoveryGroupPort()) &&
                 Util.compare(userName, you.getUserName()) &&
                 Util.compare(password, you.getPassword()) &&
                 Util.compare(clientID, you.getClientID()) &&
                 Util.compare(useXA, you.getUseXA()) &&
                 Util.compare(useTryLock, you.getUseTryLock()));
      }
    
      return false;
   }
  
   /**
    * Return the hash code for the object
    * @return The hash code
    */
   public int hashCode()
   {
      if (trace)
         log.trace("hashCode()");

      int hash = 7;

      hash += 31 * hash + (discoveryGroupName != null ? discoveryGroupName.hashCode() : 0);
      hash += 31 * hash + (discoveryGroupPort != null ? discoveryGroupPort.hashCode() : 0);
      hash += 31 * hash + (userName != null ? userName.hashCode() : 0);
      hash += 31 * hash + (password != null ? password.hashCode() : 0);
      hash += 31 * hash + (clientID != null ? clientID.hashCode() : 0);
      hash += 31 * hash + (useXA != null ? useXA.hashCode() : 0);
      hash += 31 * hash + (useTryLock != null ? useTryLock.hashCode() : 0);

      return hash;
   }
}
