/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
public class MessagingXAResourceRecovery implements XAResourceRecovery
{
   private boolean trace = log.isTraceEnabled();

   private static final Logger log = Logger.getLogger(MessagingXAResourceRecovery.class);
   
   private boolean hasMore;
   
   private MessagingXAResourceWrapper res;

   public MessagingXAResourceRecovery()
   {
      if(trace) log.trace("Constructing MessagingXAResourceRecovery");
   }

   public boolean initialise(String config)
   {
      if (log.isTraceEnabled()) { log.trace(this + " intialise: " + config); }
      
      ConfigParser parser = new ConfigParser(config);
      String connectorFactoryClassName = parser.getConnectorFactoryClassName();
      Map<String, Object> connectorParams = parser.getConnectorParameters();
      String username = parser.getUsername();
      String password = parser.getPassword();
      
      res = new MessagingXAResourceWrapper(connectorFactoryClassName, connectorParams, username, password);
             
      if (log.isTraceEnabled()) { log.trace(this + " initialised"); }      
      
      return true;      
   }

   public boolean hasMoreResources()
   {
      if (log.isTraceEnabled()) { log.trace(this + " hasMoreResources"); }
                        
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
      if (log.isTraceEnabled()) { log.trace(this + " getXAResource"); }
      
      return res;
   }

   public XAResource[] getXAResources()
   {
      return new XAResource[]{res};
   }

   protected void finalize()
   {
      res.close();  
   }
   
   public static class ConfigParser
   {
      private String connectorFactoryClassName;
      
      private Map<String, Object> connectorParameters;

      private String username;
      
      private String password;

      public ConfigParser(String config)
      {
         if (config == null || config.length() == 0)
         {
            throw new IllegalArgumentException("Must specify provider connector factory class name in config");
         }
         
         String[] strings = config.split(",");
         
         //First (mandatory) param is the  connector factory class name
         if (strings.length < 1)
         {
            throw new IllegalArgumentException("Must specify provider connector factory class name in config");
         }
         
         connectorFactoryClassName = strings[0].trim();
                     
         //Next two (optional) parameters are the username and password to use for creating the session for recovery
         
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

