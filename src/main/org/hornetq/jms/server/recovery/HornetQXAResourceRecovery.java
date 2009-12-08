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

package org.hornetq.jms.server.recovery;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

import org.hornetq.core.logging.Logger;

/**
 * 
 * A XAResourceRecovery instance that can be used to recover any JMS provider.
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class HornetQXAResourceRecovery implements XAResourceRecovery
{
   private final boolean trace = HornetQXAResourceRecovery.log.isTraceEnabled();

   private static final Logger log = Logger.getLogger(HornetQXAResourceRecovery.class);

   private boolean hasMore;

   private HornetQXAResourceWrapper res;

   public HornetQXAResourceRecovery()
   {
      if (trace)
      {
         HornetQXAResourceRecovery.log.trace("Constructing HornetQXAResourceRecovery");
      }
   }

   public boolean initialise(final String config)
   {
      if (HornetQXAResourceRecovery.log.isTraceEnabled())
      {
         HornetQXAResourceRecovery.log.trace(this + " intialise: " + config);
      }

      ConfigParser parser = new ConfigParser(config);
      String connectorFactoryClassName = parser.getConnectorFactoryClassName();
      Map<String, Object> connectorParams = parser.getConnectorParameters();
      String username = parser.getUsername();
      String password = parser.getPassword();

      res = new HornetQXAResourceWrapper(connectorFactoryClassName, connectorParams, username, password);

      if (HornetQXAResourceRecovery.log.isTraceEnabled())
      {
         HornetQXAResourceRecovery.log.trace(this + " initialised");
      }

      return true;
   }

   public boolean hasMoreResources()
   {
      if (HornetQXAResourceRecovery.log.isTraceEnabled())
      {
         HornetQXAResourceRecovery.log.trace(this + " hasMoreResources");
      }

      /*
       * The way hasMoreResources is supposed to work is as follows:
       * For each "sweep" the recovery manager will call hasMoreResources, then if it returns
       * true it will call getXAResource.
       * It will repeat that until hasMoreResources returns false.
       * Then the sweep is over.
       * For the next sweep hasMoreResources should return true, etc.
       * 
       * In our case where we only need to return one XAResource per sweep,
       * hasMoreResources should basically alternate between true and false.
       * 
       * 
       */

      hasMore = !hasMore;

      return hasMore;
   }

   public XAResource getXAResource()
   {
      if (HornetQXAResourceRecovery.log.isTraceEnabled())
      {
         HornetQXAResourceRecovery.log.trace(this + " getXAResource");
      }

      return res;
   }

   public XAResource[] getXAResources()
   {
      return new XAResource[] { res };
   }

   @Override
   protected void finalize()
   {
      res.close();
   }

   public static class ConfigParser
   {
      private final String connectorFactoryClassName;

      private final Map<String, Object> connectorParameters;

      private String username;

      private String password;

      public ConfigParser(final String config)
      {
         if (config == null || config.length() == 0)
         {
            throw new IllegalArgumentException("Must specify provider connector factory class name in config");
         }

         String[] strings = config.split(",");

         // First (mandatory) param is the connector factory class name
         if (strings.length < 1)
         {
            throw new IllegalArgumentException("Must specify provider connector factory class name in config");
         }

         connectorFactoryClassName = strings[0].trim();

         // Next two (optional) parameters are the username and password to use for creating the session for recovery

         if (strings.length >= 2)
         {

            username = strings[1].trim();
            if (username.length() == 0)
            {
               username = null;
            }

            if (strings.length == 2)
            {
               throw new IllegalArgumentException("If username is specified, password must be specified too");
            }

            password = strings[2].trim();
            if (password.length() == 0)
            {
               password = null;
            }
         }

         // other tokens are for connector configurations
         connectorParameters = new HashMap<String, Object>();
         if (strings.length >= 3)
         {
            for (int i = 3; i < strings.length; i++)
            {
               String[] str = strings[i].split("=");
               if (str.length == 2)
               {
                  connectorParameters.put(str[0].trim(), str[1].trim());
               }
            }
         }
      }

      public String getConnectorFactoryClassName()
      {
         return connectorFactoryClassName;
      }

      public Map<String, Object> getConnectorParameters()
      {
         return connectorParameters;
      }

      public String getUsername()
      {
         return username;
      }

      public String getPassword()
      {
         return password;
      }
   }
}
