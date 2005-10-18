/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
   
   public BaseJob(String serverURL, String destinationName)
   {
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
      
      dest = (Destination)ic.lookup(destName);
      
      cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
      
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
   
//   public String getLocator()
//   {
//      return slaveLocator;
//   }
}
