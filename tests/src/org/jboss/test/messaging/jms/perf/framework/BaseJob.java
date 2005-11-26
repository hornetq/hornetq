/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

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
   
   protected Properties jndiProperties;
   
   protected String destName;
   
   protected Destination dest;
   
   protected InitialContext ic;
   
   protected ConnectionFactory cf;
   
   protected boolean failed;
   
   protected String slaveURL;
   
   protected String connectionFactoryJndiName;
   
   public BaseJob(String slaveURL, Properties jndiProperties, String destinationName, String connectionFactoryJndiName)
   {
      this.slaveURL = slaveURL;
      this.jndiProperties = jndiProperties;
      this.destName = destinationName;
      this.connectionFactoryJndiName = connectionFactoryJndiName;
   }
 
   
   protected void setup() throws Exception
   { 
      ic = new InitialContext(jndiProperties);
      
      log.trace("Looking up:" + destName);
      
      dest = (Destination)ic.lookup(destName);
      
      cf = (ConnectionFactory)ic.lookup(connectionFactoryJndiName);
      
   }
   
   protected void logInfo()
   {
      //log.debug("Server-url: " + this.serverURL);
      log.debug("Destination name: " + this.destName);
   }
   
   protected void tearDown() throws Exception
   {
      ic.close();
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
   public void setConnectionFactoryJndiName(String connectionFactoryJndiName)
   {
      this.connectionFactoryJndiName = connectionFactoryJndiName;
   }
   
   public void setSlaveURL(String slaveURL)
   {
      this.slaveURL = slaveURL;
   }
   
   public String getSlaveURL()
   {
      return slaveURL;
   }
   
}
