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

import java.io.Serializable;

import javax.jms.Queue;
import javax.jms.Topic;

import org.hornetq.core.logging.Logger;

/**
 * The MCF default properties - these are set in the <tx-connection-factory> at the jms-ds.xml
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class HornetQRAMCFProperties extends ConnectionFactoryProperties implements Serializable
{
   /**
    * Serial version UID
    */
   static final long serialVersionUID = -5951352236582886862L;

   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(HornetQRAMCFProperties.class);

   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRAMCFProperties.log.isTraceEnabled();

   /**
    * The queue type
    */
   private static final String QUEUE_TYPE = Queue.class.getName();

   /**
    * The topic type
    */
   private static final String TOPIC_TYPE = Topic.class.getName();


   private String strConnectorClassName;

   public String strConnectionParameters;

   /**
    * The connection type
    */
   private int type = HornetQRAConnectionFactory.CONNECTION;

   /**
    * Use tryLock
    */
   private Integer useTryLock;

   /**
    * Constructor
    */
   public HornetQRAMCFProperties()
   {
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("constructor()");
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
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("getType()");
      }

      return type;
   }

   public String getConnectorClassName()
   {
      return strConnectorClassName;
   }

   public void setConnectorClassName(final String connectorClassName)
   {
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("setConnectorClassName(" + connectorClassName + ")");
      }

      strConnectorClassName = connectorClassName;

      setParsedConnectorClassNames(Util.parseConnectorConnectorConfig(connectorClassName));
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
    * Set the default session type.
    *
    * @param defaultType either javax.jms.Topic or javax.jms.Queue
    */
   public void setSessionDefaultType(final String defaultType)
   {
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("setSessionDefaultType(" + type + ")");
      }

      if (defaultType.equals(HornetQRAMCFProperties.QUEUE_TYPE))
      {
         type = HornetQRAConnectionFactory.QUEUE_CONNECTION;
      }
      else if (defaultType.equals(HornetQRAMCFProperties.TOPIC_TYPE))
      {
         type = HornetQRAConnectionFactory.TOPIC_CONNECTION;
      }
      else
      {
         type = HornetQRAConnectionFactory.CONNECTION;
      }
   }

   /**
    * Get the default session type.
    *
    * @return The default session type
    */
   public String getSessionDefaultType()
   {
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("getSessionDefaultType()");
      }

      if (type == HornetQRAConnectionFactory.CONNECTION)
      {
         return "BOTH";
      }
      else if (type == HornetQRAConnectionFactory.QUEUE_CONNECTION)
      {
         return HornetQRAMCFProperties.TOPIC_TYPE;
      }
      else
      {
         return HornetQRAMCFProperties.QUEUE_TYPE;
      }
   }

   /**
    * Get the useTryLock.
    *
    * @return the useTryLock.
    */
   public Integer getUseTryLock()
   {
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("getUseTryLock()");
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
      if (HornetQRAMCFProperties.trace)
      {
         HornetQRAMCFProperties.log.trace("setUseTryLock(" + useTryLock + ")");
      }

      this.useTryLock = useTryLock;
   }
}
