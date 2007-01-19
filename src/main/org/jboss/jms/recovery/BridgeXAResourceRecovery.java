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

import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * 
 * A BridgeXAResourceRecovery
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeXAResourceRecovery implements XAResourceRecovery
{
   private boolean trace = log.isTraceEnabled();

   private static final Logger log = Logger.getLogger(BridgeXAResourceRecovery.class);

   private Hashtable jndiProperties;
   
   private String connectionFactoryLookup;
   
   private boolean hasMore;
   
   private String username;
   
   private String password;
   
   private XAConnection conn;
   
   private XAResource res;

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
          * provider1.username=bob
          * provider1.password=blah
          * 
          * provider2.jndi.prop1=xxxx
          * provider2.jndi.prop2=yyyy
          * provider2.jndi.prop3=zzzz
          * 
          * provider2.xaconnectionfactorylookup=xyz
          * provider2.username=xyz
          * provider2.password=blah
          *           
          */
         
         Iterator iter = props.entrySet().iterator();
         
         String jndiPrefix = provider + ".jndi.";
         
         String cfKey = provider + ".xaconnectionfactorylookup";
         
         String usernameKey = provider + ".username";
         
         String passwordKey = provider + ".password";
         
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
            else if (key.equals(usernameKey))
            {
               username = value;
            }
            else if (key.equals(passwordKey))
            {
               password = value;
            }
         }
         
         if (connectionFactoryLookup == null)
         {
            log.error("Key " + cfKey + " does not exist in config");
            return false;
         }
         
         if (log.isTraceEnabled()) { log.trace(this + " initialised"); }
         
         hasMore = true;
         
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
       * And we return a new XAResource every time it is called.
       * This makes this resilient to failure, since if the network fails
       * between the XAResource and it's server, on the next pass a new one will
       * be create and if the server is back up it will work.
       * This means there is no need for an XAResourceWrapper which is a technique used in the 
       * JMSProviderXAResourceRecovery
       * The recovery manager will throw away the XAResource anyway after every sweep.
       * 
       */
        
      if (hasMore)
      {
         //Get a new XAResource
         
         try
         {
            if (conn != null)
            {
               conn.close();
            }
         }
         catch (Exception ignore)
         {         
         }
         
         InitialContext ic = null;
         
         try
         {
            ic = new InitialContext(jndiProperties);
            
            XAConnectionFactory connectionFactory = (XAConnectionFactory)ic.lookup(connectionFactoryLookup);
            
            if (username == null)
            {
               conn = connectionFactory.createXAConnection();
            }
            else
            {
               conn = connectionFactory.createXAConnection(username, password);
            }
            
            XASession session = conn.createXASession();
            
            res = session.getXAResource();
            
            //Note the connection is closed the next time the xaresource is created or by the finalizer
            
         }
         catch (Exception e)
         {
            log.warn("Cannot create XAResource", e);
            
            hasMore = false;
         }
         finally
         {
            if (ic != null)
            {
               try
               {
                  ic.close();
               }
               catch (Exception ignore)
               {               
               }
            }
         }
         
      }
      
      boolean ret = hasMore;
            
      hasMore = !hasMore;
      
      return ret;      
   }

   public XAResource getXAResource()
   {
      if (log.isTraceEnabled()) { log.trace(this + " getXAResource"); }
      
      return res;
   }
   
   protected void finalize()
   {
      try
      {
         if (conn != null)
         {
            conn.close();
         }
      }
      catch (Exception ignore)
      {         
      }  
   }
}

