/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.io.Serializable;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

/**
 * 
 * A BaseJob.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public abstract class BaseJob implements Job, Serializable
{
   private transient static final Logger log = Logger.getLogger(BaseJob.class);
   
   protected Properties jndiProperties;
   
   protected String destName;
   
   protected Destination dest;
   
   protected InitialContext ic;
   
   protected ConnectionFactory cf;
   
   protected String connectionFactoryJndiName;
   
   protected String id;
   
   public BaseJob(Properties jndiProperties, String destinationName, String connectionFactoryJndiName)
   {
      this.jndiProperties = jndiProperties;
      this.destName = destinationName;
      this.connectionFactoryJndiName = connectionFactoryJndiName;
      this.id = new GUID().toString();
   }
 
   
   public void initialize() throws PerfException
   { 
      try
      {         
         ic = new InitialContext(jndiProperties);
         
         log.trace("Looking up:" + destName);
         
         dest = (Destination)ic.lookup(destName);
         
         cf = (ConnectionFactory)ic.lookup(connectionFactoryJndiName);
         
      }
      catch (Exception e)
      {
         log.error("Failed to initialize", e);
         throw new PerfException("Failed to initialize", e);
      }
      
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
   
   public String getId()
   {
      return id;
   }
   
}
