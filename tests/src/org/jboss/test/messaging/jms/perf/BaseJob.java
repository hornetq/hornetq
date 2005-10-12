/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

public abstract class BaseJob implements Job, Serializable
{
   private transient static final Logger log = Logger.getLogger(BaseJob.class);
   
   protected String serverURL;
   
   protected String destName;
   
   protected Destination dest;
   
   protected InitialContext ic;
   
   protected ConnectionFactory cf;
   
   protected boolean failed;
   
   public void importXML(Element element) throws ConfigurationException
   { 
      if (log.isTraceEnabled()) { log.trace("importing xml"); }
      this.serverURL = MetadataUtils.getUniqueChildContent(element, "jms-server-url");
      this.destName = MetadataUtils.getUniqueChildContent(element, "destination-jndi-name");
      if (log.isTraceEnabled()) { log.trace("server url iz:" + this.serverURL); }
   }
   
   public BaseJob()
   {
      
   }
   
   
   
   public BaseJob(Element el)
   {
      try
      {
         importXML(el);
      }
      catch (ConfigurationException e)
      {
         log.error("Failed to inport XML", e);
      }
      
      logInfo();
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
}
