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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.jms.server.HornetQJMSServerLogger;

/**
 *
 * A XAResourceRecovery instance that can be used to recover any JMS provider.
 *
 * In reality only recover,rollback and commit will be called but we still need to
 * be implement all methods just in case.
 *
 * to enable this add the following to the jbossts-properties file
 *
 * <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.HORNETQ1"
 *                 value="org.hornetq.jms.server.recovery.HornetQXAResourceRecovery;org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"/>
 *
 * you'll need something like this if the HornetQ Server is remote
 *
 *      <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.HORNETQ2"
 *                  value="org.hornetq.jms.server.recovery.HornetQXAResourceRecovery;org.hornetq.core.remoting.impl.netty.NettyConnectorFactory,guest,guest,host=localhost,port=5445"/>-->
 *
 * you'll need something like this if the HornetQ Server is remote and has failover configured-->
 *
 *             <property name="com.arjuna.ats.jta.recovery.XAResourceRecovery.HORNETQ2"
 *                       value="org.hornetq.jms.server.recovery.HornetQXAResourceRecovery;org.hornetq.core.remoting.impl.netty.NettyConnectorFactory,guest,guest,host=localhost,port=5445;org.hornetq.core.remoting.impl.netty.NettyConnectorFactory,guest,guest,host=localhost2,port=5446"/>-->
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class HornetQXAResourceRecovery implements XAResourceRecovery
{
   private final boolean trace = HornetQJMSServerLogger.LOGGER.isTraceEnabled();

   private boolean hasMore;

   private HornetQXAResourceWrapper res;

   public HornetQXAResourceRecovery()
   {
      if (trace)
      {
         HornetQJMSServerLogger.LOGGER.trace("Constructing HornetQXAResourceRecovery");
      }
   }

   public boolean initialise(final String config)
   {
      if (HornetQJMSServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQJMSServerLogger.LOGGER.trace(this + " intialise: " + config);
      }

      String[] configs = config.split(";");
      XARecoveryConfig[] xaRecoveryConfigs = new XARecoveryConfig[configs.length];
      for (int i = 0, configsLength = configs.length; i < configsLength; i++)
      {
         String s = configs[i];
         ConfigParser parser = new ConfigParser(s);
         String connectorFactoryClassName = parser.getConnectorFactoryClassName();
         Map<String, Object> connectorParams = parser.getConnectorParameters();
         String username = parser.getUsername();
         String password = parser.getPassword();
         TransportConfiguration transportConfiguration = new TransportConfiguration(connectorFactoryClassName, connectorParams);
         xaRecoveryConfigs[i] = new XARecoveryConfig(false, new TransportConfiguration[]{transportConfiguration}, username, password);
      }



      res = new HornetQXAResourceWrapper(xaRecoveryConfigs);

      if (HornetQJMSServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQJMSServerLogger.LOGGER.trace(this + " initialised");
      }

      return true;
   }

   public boolean hasMoreResources()
   {
      if (HornetQJMSServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQJMSServerLogger.LOGGER.trace(this + " hasMoreResources");
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
      if (HornetQJMSServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQJMSServerLogger.LOGGER.trace(this + " getXAResource");
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
