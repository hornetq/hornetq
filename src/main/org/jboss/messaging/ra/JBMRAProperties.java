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

import org.jboss.messaging.core.logging.Logger;

import java.io.Serializable;

/**
 * The RA default properties - these are set in the ra.xml file
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class JBMRAProperties extends ConnectionFactoryProperties implements Serializable
{
   /** Serial version UID */
   static final long serialVersionUID = -2772367477755473248L;

   /** The logger */
   private static final Logger log = Logger.getLogger(JBMRAProperties.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /** Use XA */
   private Boolean useXA;

   /** Use Local TX instead of XA */
   private Boolean localTx = false;

   /**
    * Constructor
    */
   public JBMRAProperties()
   {
      if (trace)
      {
         log.trace("constructor()");
      }
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
    * Get the use XA flag
    * @return The value
    */
   public Boolean getUseLocalTx()
   {
      if (trace)
      {
         log.trace("getUseLocalTx()");
      }

      return localTx;
   }

   /**
    * Set the use XA flag
    * @param localTx The value
    */
   public void setUseLocalTx(final Boolean localTx)
   {
      if (trace)
      {
         log.trace("setUseLocalTx(" + localTx + ")");
      }

      this.localTx = localTx;
   }

   /**
    * Get the use XA flag
    * @return The value
    */
   public Boolean getUseXA()
   {
      if (trace)
      {
         log.trace("getUseXA()");
      }

      return useXA;
   }

   /**
    * Set the use XA flag
    * @param xa The value
    */
   public void setUseXA(final Boolean xa)
   {
      if (trace)
      {
         log.trace("setUseXA(" + xa + ")");
      }

      useXA = xa;
   }

   /**
    * Use XA for communication
    * @return The value
    */
   public boolean isUseXA()
   {
      if (trace)
      {
         log.trace("isUseXA()");
      }

      return useXA != null && useXA;
   }
   
}
