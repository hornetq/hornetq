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

package org.hornetq.ra;

import javax.jms.Session;
import javax.resource.spi.ConnectionRequestInfo;

import org.hornetq.core.logging.Logger;

/**
 * Connection request information
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision:  $
 */
public class HornetQRAConnectionRequestInfo implements ConnectionRequestInfo
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRAConnectionRequestInfo.class);

   /** Trace enabled */
   private static boolean trace = HornetQRAConnectionRequestInfo.log.isTraceEnabled();

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /** The client id */
   private String clientID;

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
   public HornetQRAConnectionRequestInfo(final HornetQRAProperties prop, final int type)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("constructor(" + prop + ")");
      }

      userName = prop.getUserName();
      password = prop.getPassword();
      clientID = prop.getClientID();
      this.type = type;
      transacted = true;
      acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
   }

   /**
    * Constructor
    * @param type The connection type
    */
   public HornetQRAConnectionRequestInfo(final int type)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("constructor(" + type + ")");
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
   public HornetQRAConnectionRequestInfo(final boolean transacted, final int acknowledgeMode, final int type)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("constructor(" + transacted +
                                                  ", " +
                                                  acknowledgeMode +
                                                  ", " +
                                                  type +
                                                  ")");
      }

      this.transacted = transacted;
      this.acknowledgeMode = acknowledgeMode;
      this.type = type;
   }

   /**
    * Fill in default values if they are missing
    * @param prop The resource adapter properties
    */
   public void setDefaults(final HornetQRAProperties prop)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("setDefaults(" + prop + ")");
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
   }

   /**
    * Get the user name
    * @return The value
    */
   public String getUserName()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    * @param userName The value
    */
   public void setUserName(final String userName)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    * @param password The value
    */
   public void setPassword(final String password)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * Get the client id
    * @return The value
    */
   public String getClientID()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("getClientID()");
      }

      return clientID;
   }

   /**
    * Set the client id
    * @param clientID The value
    */
   public void setClientID(final String clientID)
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("setClientID(" + clientID + ")");
      }

      this.clientID = clientID;
   }

   /**
    * Get the connection type
    * @return The type
    */
   public int getType()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("getType()");
      }

      return type;
   }

   /**
    * Use transactions
    * @return True if transacted; otherwise false
    */
   public boolean isTransacted()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("isTransacted() " + transacted);
      }

      return transacted;
   }

   /**
    * Get the acknowledge mode
    * @return The mode
    */
   public int getAcknowledgeMode()
   {
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("getAcknowledgeMode()");
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
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("equals(" + obj + ")");
      }

      if (obj == null)
      {
         return false;
      }

      if (obj instanceof HornetQRAConnectionRequestInfo)
      {
         HornetQRAConnectionRequestInfo you = (HornetQRAConnectionRequestInfo)obj;
         return Util.compare(userName, you.getUserName()) && Util.compare(password, you.getPassword()) &&
                Util.compare(clientID, you.getClientID()) &&
                type == you.getType() &&
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
      if (HornetQRAConnectionRequestInfo.trace)
      {
         HornetQRAConnectionRequestInfo.log.trace("hashCode()");
      }

      int hash = 7;

      hash += 31 * hash + (userName != null ? userName.hashCode() : 0);
      hash += 31 * hash + (password != null ? password.hashCode() : 0);
      hash += 31 * hash + Integer.valueOf(type).hashCode();
      hash += 31 * hash + (transacted ? 1 : 0);
      hash += 31 * hash + Integer.valueOf(acknowledgeMode).hashCode();

      return hash;
   }
   
   @Override
   public String toString()
   {
      return "HornetQRAConnectionRequestInfo[type=" + type +
         ", transacted=" + transacted + ", acknowledgeMode=" + acknowledgeMode +
         ", clientID=" + clientID + ", userName=" + userName + ", password=" + password + "]";
   }
}
