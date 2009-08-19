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
package org.hornetq.ra;

import org.hornetq.core.logging.Logger;

import javax.jms.Queue;
import javax.jms.Topic;
import java.io.Serializable;

/**
 * The MCF default properties - these are set in the <tx-connection-factory> at the jms-ds.xml
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class JBMMCFProperties extends ConnectionFactoryProperties implements Serializable
{
   /**
    * Serial version UID
    */
   static final long serialVersionUID = -5951352236582886862L;

   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(JBMMCFProperties.class);

   /**
    * Trace enabled
    */
   private static boolean trace = log.isTraceEnabled();

   /**
    * The queue type
    */
   private static final String QUEUE_TYPE = Queue.class.getName();

   /**
    * The topic type
    */
   private static final String TOPIC_TYPE = Topic.class.getName();



   public String strConnectionParameters;

   public String strBackupConnectionParameters;

   /**
    * The connection type
    */
   private int type = JBMConnectionFactory.CONNECTION;

   /**
    * Use tryLock
    */
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
    *
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
   public String getStrConnectionParameters()
   {
      return strConnectionParameters;
   }


   public void setConnectionParameters(final String configuration)
   {
      strConnectionParameters = configuration;
      setParsedConnectionParameters(Util.parseConfig(configuration));
   }

   /**
    * @return the connectionParameters
    */
   public String getBackupConnectionParameters()
   {
      return strBackupConnectionParameters;
   }

   public void setBackupConnectionParameters(final String configuration)
   {
      strBackupConnectionParameters = configuration;
      setParsedBackupConnectionParameters(Util.parseConfig(configuration));
   }
   
   /**
    * Set the default session type.
    *
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
    *
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
    *
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
    *
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
}
