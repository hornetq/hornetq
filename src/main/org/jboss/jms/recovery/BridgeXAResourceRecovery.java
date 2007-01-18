/*
* JBoss, Home of Professional Open Source
* Copyright 2006, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.recovery;

import java.io.InputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="juha@jboss.com">Juha Lindfors</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 * @version $Revision: 1.1 $
 */
public class BridgeXAResourceRecovery implements XAResourceRecovery
{
   private boolean trace = log.isTraceEnabled();

   private static final Logger log = Logger.getLogger(BridgeXAResourceRecovery.class);

   private BridgeXAResourceWrapper wrapper;

   private boolean working = false;
   
   private Hashtable jndiProperties;
   
   private String connectionFactoryLookup;

   public BridgeXAResourceRecovery()
   {
      if(trace) log.trace("Constructing BridgeXAResourceRecovery2..");
   }

   public boolean initialise(String config)
   {
      if (log.isTraceEnabled()) { log.trace(this + " intialise: " + config); }
      
      StringTokenizer tok = new StringTokenizer(config, ",");
      
      if (tok.countTokens() != 2)
      {
         log.error("Invalid config: " + config);
         return false;
      }
      
      String provider = tok.nextToken();
      
      String propsFile = tok.nextToken();
      
      try
      {
         //The config should point to a properties file on the classpath that holds the actual config
         InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(propsFile);
         
         Properties props = new Properties();
         
         props.load(is);
         
         /*
          * provider1.jndi.prop1=xxxx
          * provider1.jndi.prop2=yyyy
          * provider1.jndi.prop3=zzzz
          * 
          * provider1.xaconnectionfactorylookup=xyz
          * 
          * provider2.jndi.prop1=xxxx
          * provider2.jndi.prop2=yyyy
          * provider2.jndi.prop3=zzzz
          * 
          * provider2.xaconnectionfactorylookup=xyz
          *           
          */
         
         Iterator iter = props.entrySet().iterator();
         
         String jndiPrefix = provider + ".jndi.";
         
         String cfKey = provider + ".xaconnectionfactorylookup";
         
         jndiProperties = new Hashtable();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            
            if (key.startsWith(jndiPrefix))
            {
               String actualKey = key.substring(jndiPrefix.length());
               
               jndiProperties.put(actualKey, value);
            }
            else if (key.equals(cfKey))
            {
               connectionFactoryLookup = value;
            }
         }
         
         if (connectionFactoryLookup == null)
         {
            log.error("Key " + cfKey + " does not exist in config");
            return false;
         }
         
         if (log.isTraceEnabled()) { log.trace(this + " initialised"); }
         
         return true;
      }
      catch (Exception e)
      {
         log.error("Failed to load config file: " + config, e);
         
         return false;
      }
   }

   public boolean hasMoreResources()
   {
      if (log.isTraceEnabled()) { log.trace(this + " hasMoreResources"); }
            
      // If the XAResource is already working
      if (working)
      {
         log.info("Returning false");
         return false;
      }

      // Have we initialized yet?
      if (wrapper == null)
      {
         wrapper = new BridgeXAResourceWrapper(jndiProperties, connectionFactoryLookup);
      }

      // Test the connection
      try
      {
         wrapper.getTransactionTimeout();
         working = true;
      }
      catch (Exception ignored)
      {
      }
      
      log.info("Returning: " + working);

      // This will return false until we get a successful connection
      return working;
   }

   public XAResource getXAResource()
   {
      if (log.isTraceEnabled()) { log.trace(this + " getXAResource"); }
      
      return wrapper;
   }
}

