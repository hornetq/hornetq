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
import java.util.Map;

import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.messaging.core.logging.Logger;

/**
 * The MCF default properties - these are set in the <tx-connection-factory> at the jms-ds.xml
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version $Revision: $
 */
public class JBMMCFProperties implements Serializable
{
   /** Serial version UID */
   static final long serialVersionUID = -5951352236582886862L;

   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMCFProperties.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The queue type */
   private static final String QUEUE_TYPE = Queue.class.getName();

   /** The topic type */
   private static final String TOPIC_TYPE = Topic.class.getName();

   /** The transport config, changing the default configured from the RA */
   private Map<String, Object> connectionParameters;

   public String strConnectionParameters;

   /** The transport type, changing the default configured from the RA */
   private String connectorClassName;

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
      {
         log.trace("constructor()");
      }

      useTryLock = null;
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

   public void setConnectionParameters(final String configuration)
   {
      strConnectionParameters = configuration;
      connectionParameters = Util.parseConfig(configuration);
   }

   /**
    * @return the transportType
    */
   public String getConnectorClassName()
   {
      return connectorClassName;
   }

   public void setConnectorClassName(final String value)
   {
      connectorClassName = value;
   }

   /**
    * Set the default session type.
    * @param defaultType either javax.jms.Topic or javax.jms.Queue
    */
   public void setSessionDefaultType(final String defaultType)
   {
      if (trace)
      {
         log.trace("setSessionDefaultType(" + type + ")");
      }

      if (defaultType.equals(QUEUE_TYPE))
      {
         type = JBMConnectionFactory.QUEUE_CONNECTION;
      }
      else if (defaultType.equals(TOPIC_TYPE))
      {
         type = JBMConnectionFactory.TOPIC_CONNECTION;
      }
      else
      {
         type = JBMConnectionFactory.CONNECTION;
      }
   }

   /**
    * Get the default session type.
    * @return The default session type
    */
   public String getSessionDefaultType()
   {
      if (trace)
      {
         log.trace("getSessionDefaultType()");
      }

      if (type == JBMConnectionFactory.CONNECTION)
      {
         return "BOTH";
      }
      else if (type == JBMConnectionFactory.QUEUE_CONNECTION)
      {
         return TOPIC_TYPE;
      }
      else
      {
         return QUEUE_TYPE;
      }
   }

   /**
    * Get the useTryLock.
    * @return the useTryLock.
    */
   public Integer getUseTryLock()
   {
      if (trace)
      {
         log.trace("getUseTryLock()");
      }

      return useTryLock;
   }

   /**
    * Set the useTryLock.
    * @param useTryLock the useTryLock.
    */
   public void setUseTryLock(final Integer useTryLock)
   {
      if (trace)
      {
         log.trace("setUseTryLock(" + useTryLock + ")");
      }

      this.useTryLock = useTryLock;
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

      if (obj instanceof JBMMCFProperties)
      {
         JBMMCFProperties you = (JBMMCFProperties)obj;
         return type == you.getType() && Util.compare(useTryLock, you.getUseTryLock());
      }

      return false;
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

      hash += 31 * hash + Integer.valueOf(type).hashCode();
      hash += 31 * hash + (useTryLock != null ? useTryLock.hashCode() : 0);

      return hash;
   }
}
