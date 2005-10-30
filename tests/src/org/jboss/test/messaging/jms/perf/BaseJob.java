/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class BaseJob implements Job, Serializable
{
   private transient static final Logger log = Logger.getLogger(BaseJob.class);
   
   protected String serverURL;
   
   protected String destName;
   
   protected Destination dest;
   
   protected InitialContext ic;
   
   protected ConnectionFactory cf;
   
   protected boolean failed;
   
   protected String slaveURL;
   
   public BaseJob(String slaveURL, String serverURL, String destinationName)
   {
      this.slaveURL = slaveURL;
      this.serverURL = serverURL;
      this.destName = destinationName;
   }
   

   protected String sub(Map variables, String value)
   {
      if (value == null)
      {
         return null;
      }
      
      if (variables == null)
      {
         return value;
      }
      
      value = value.trim();
      
      if (value.startsWith("@"))
      {
         String variable = value.substring(1);
         String newValue = (String)variables.get(variable);
         if (newValue != null)
         {
            value = newValue;
         }
      }
      return value;
   }
   
   protected void setup() throws Exception
   {
      //Hardcoded for now - needs to be configurable if it's to work with non jboss
      //jms providers
      
      if (log.isTraceEnabled())
      {
         log.trace("server url is " + serverURL);
      }

      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      env.put("java.naming.provider.url", serverURL);
      env.put("java.naming.factory.url.pkg", "org.jboss.naming:org.jnp.interfaces");
      
      ic = new InitialContext(env);
      
      log.trace("Looking up:" + destName);
      
      dest = (Destination)ic.lookup(destName);
      
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
   }
   
   protected void logInfo()
   {
      log.debug("Server-url: " + this.serverURL);
      log.debug("Destination name: " + this.destName);
   }
   
   protected void tearDown() throws Exception
   {
      ic.close();
   }
   
   public boolean isFailed()
   {
      return failed;
   }

   /**
    * Set the destName.
    * 
    * @param destName The destName to set.
    */
   public void setDestName(String destName)
   {
      this.destName = destName;
   }

   

   /**
    * Set the serverURL.
    * 
    * @param serverURL The serverURL to set.
    */
   public void setServerURL(String serverURL)
   {
      this.serverURL = serverURL;
   }
   
   public void setSlaveURL(String slaveURL)
   {
      this.slaveURL = slaveURL;
   }
   
   public String getSlaveURL()
   {
      return slaveURL;
   }
   
//   public String getLocator()
//   {
//      return slaveLocator;
//   }
}
