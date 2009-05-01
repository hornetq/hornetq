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

import javax.jms.Session;
import javax.resource.spi.ConnectionRequestInfo;

import org.jboss.messaging.core.logging.Logger;

/**
 * Connection request information
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision:  $
 */
public class JBMConnectionRequestInfo implements ConnectionRequestInfo
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMConnectionRequestInfo.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /** The client id */
   private String clientID;

   /** Use XA */
   private boolean useXA;

   /** The type */
   private final int type;

   /** Use transactions */
   private final boolean transacted;

   /** The acknowledge mode */
   private final int acknowledgeMode;

   /**
    * Constructor
    * @param prop The resource adapter properties
    * @param type The connection type
    */
   public JBMConnectionRequestInfo(final JBMRAProperties prop, final int type)
   {
      if (trace)
      {
         log.trace("constructor(" + prop + ")");
      }

      userName = prop.getUserName();
      password = prop.getPassword();
      clientID = prop.getClientID();
      useXA = prop.isUseXA();
      this.type = type;
      transacted = true;
      acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
   }

   /**
    * Constructor
    * @param type The connection type
    */
   public JBMConnectionRequestInfo(final int type)
   {
      if (trace)
      {
         log.trace("constructor(" + type + ")");
      }

      this.type = type;
      transacted = true;
      acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
   }

   /**
    * Constructor
    * @param transacted Use transactions
    * @param acknowledgeMode The acknowledge mode
    * @param type The connection type
    */
   public JBMConnectionRequestInfo(final boolean transacted, final int acknowledgeMode, final int type)
   {
      if (trace)
      {
         log.trace("constructor(" + transacted + ", " + acknowledgeMode + ", " + type + ")");
      }

      this.transacted = transacted;
      this.acknowledgeMode = acknowledgeMode;
      this.type = type;
   }

   /**
    * Fill in default values if they are missing
    * @param prop The resource adapter properties
    */
   public void setDefaults(final JBMRAProperties prop)
   {
      if (trace)
      {
         log.trace("setDefaults(" + prop + ")");
      }

      if (userName == null)
      {
         userName = prop.getUserName();
      }
      if (password == null)
      {
         password = prop.getPassword();
      }
      if (clientID == null)
      {
         clientID = prop.getClientID();
      }
      useXA = prop.isUseXA();
   }

   /**
    * Get the user name
    * @return The value
    */
   public String getUserName()
   {
      if (trace)
      {
         log.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    * @param userName The value
    */
   public void setUserName(final String userName)
   {
      if (trace)
      {
         log.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (trace)
      {
         log.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    * @param password The value
    */
   public void setPassword(final String password)
   {
      if (trace)
      {
         log.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * Get the client id
    * @return The value
    */
   public String getClientID()
   {
      if (trace)
      {
         log.trace("getClientID()");
      }

      return clientID;
   }

   /**
    * Set the client id
    * @param clientID The value
    */
   public void setClientID(final String clientID)
   {
      if (trace)
      {
         log.trace("setClientID(" + clientID + ")");
      }

      this.clientID = clientID;
   }

   /**
    * Get the connection type
    * @return The type
    */
   public int getType()
   {
      if (trace)
      {
         log.trace("getType()");
      }

      return type;
   }

   /**
    * Use XA communication
    * @return True if XA; otherwise false
    */
   public boolean isUseXA()
   {
      if (trace)
      {
         log.trace("isUseXA()");
      }

      return useXA;
   }

   /**
    * Use transactions
    * @return True if transacted; otherwise false
    */
   public boolean isTransacted()
   {
      if (trace)
      {
         log.trace("isTransacted()");
      }

      return transacted;
   }

   /**
    * Get the acknowledge mode
    * @return The mode
    */
   public int getAcknowledgeMode()
   {
      if (trace)
      {
         log.trace("getAcknowledgeMode()");
      }

      return acknowledgeMode;
   }

   /**
    * Indicates whether some other object is "equal to" this one.
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   @Override
   public boolean equals(final Object obj)
   {
      if (trace)
      {
         log.trace("equals(" + obj + ")");
      }

      if (obj == null)
      {
         return false;
      }

      if (obj instanceof JBMConnectionRequestInfo)
      {
         JBMConnectionRequestInfo you = (JBMConnectionRequestInfo)obj;
         return Util.compare(userName, you.getUserName()) && Util.compare(password, you.getPassword()) &&
                Util.compare(clientID, you.getClientID()) &&
                type == you.getType() &&
                useXA == you.isUseXA() &&
                transacted == you.isTransacted() &&
                acknowledgeMode == you.getAcknowledgeMode();
      }
      else
      {
         return false;
      }
   }

   /**
    * Return the hash code for the object
    * @return The hash code
    */
   @Override
   public int hashCode()
   {
      if (trace)
      {
         log.trace("hashCode()");
      }

      int hash = 7;

      hash += 31 * hash + (userName != null ? userName.hashCode() : 0);
      hash += 31 * hash + (password != null ? password.hashCode() : 0);
      hash += 31 * hash + Integer.valueOf(type).hashCode();
      hash += 31 * hash + (useXA ? 1 : 0);
      hash += 31 * hash + (transacted ? 1 : 0);
      hash += 31 * hash + Integer.valueOf(acknowledgeMode).hashCode();

      return hash;
   }
}
