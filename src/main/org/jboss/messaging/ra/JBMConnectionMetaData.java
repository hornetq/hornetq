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

import java.util.Enumeration;
import java.util.Vector;

import javax.jms.ConnectionMetaData;

import org.jboss.messaging.core.logging.Logger;

/**
 * This class implements javax.jms.ConnectionMetaData
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMConnectionMetaData implements ConnectionMetaData
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMConnectionMetaData.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Constructor
    */
   public JBMConnectionMetaData()
   {
      if (trace)
      {
         log.trace("constructor()");
      }
   }

   /**
    * Get the JMS version
    * @return The version
    */
   public String getJMSVersion()
   {
      if (trace)
      {
         log.trace("getJMSVersion()");
      }

      return "1.1";
   }

   /**
    * Get the JMS major version
    * @return The major version
    */
   public int getJMSMajorVersion()
   {
      if (trace)
      {
         log.trace("getJMSMajorVersion()");
      }

      return 1;
   }

   /**
    * Get the JMS minor version
    * @return The minor version
    */
   public int getJMSMinorVersion()
   {
      if (trace)
      {
         log.trace("getJMSMinorVersion()");
      }

      return 1;
   }

   /**
    * Get the JMS provider name
    * @return The name
    */
   public String getJMSProviderName()
   {
      if (trace)
      {
         log.trace("getJMSProviderName()");
      }

      return "JBoss Messaging";
   }

   /**
    * Get the provider version
    * @return The version
    */
   public String getProviderVersion()
   {
      if (trace)
      {
         log.trace("getJMSProviderName()");
      }

      return "2.0";
   }

   /**
    * Get the provider major version
    * @return The version
    */
   public int getProviderMajorVersion()
   {
      if (trace)
      {
         log.trace("getProviderMajorVersion()");
      }

      return 2;
   }

   /**
    * Get the provider minor version
    * @return The version
    */
   public int getProviderMinorVersion()
   {
      if (trace)
      {
         log.trace("getProviderMinorVersion()");
      }

      return 0;
   }

   /**
    * Get the JMS XPropertyNames
    * @return The names
    */
   public Enumeration<Object> getJMSXPropertyNames()
   {
      Vector<Object> v = new Vector<Object>();
      return v.elements();
   }
}
