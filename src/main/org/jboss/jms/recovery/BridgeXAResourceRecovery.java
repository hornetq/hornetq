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
package org.jboss.jms.recovery;

import java.io.InputStream;
import java.sql.SQLException;
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
 * A BridgeXAResourceRecovery
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeXAResourceRecovery implements XAResourceRecovery
{
   private static final Logger log = Logger.getLogger(BridgeXAResourceRecovery.class);
   
   private Hashtable jndiProperties;
   
   private String connectionFactoryLookup;
   
   private boolean returnedXAResource;
   
   private XAConnection conn;

   public synchronized XAResource getXAResource() throws SQLException
   {
      InitialContext ic = null;
      
      if (log.isTraceEnabled()) { log.trace(this + " getting XAResource"); }
      
      try
      {         
         if (jndiProperties.isEmpty())
         {
            //Local initial context
            
            ic = new InitialContext();
         }
         else
         {
            ic = new InitialContext(jndiProperties);
         }
         
         XAConnectionFactory cf = (XAConnectionFactory)ic.lookup(connectionFactoryLookup);
         
         conn = cf.createXAConnection();
         
         XASession sess = conn.createXASession();
         
         XAResource res = sess.getXAResource();
         
         returnedXAResource = true;
         
         if (log.isTraceEnabled()) { log.trace(this + " returning " + res); }
         
         return res;
      }     
      catch (Exception e)
      {
         log.warn("Failed to get XAResource", e);
         
         return null;
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
               //Ignore
            }
         }
      }
      
   }

   public synchronized boolean hasMoreResources()
   {
      return !returnedXAResource;
   }

   public synchronized boolean initialise(String config)
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
   
   protected void finalize()
   {
      if (log.isTraceEnabled()) { log.trace(this + " finalizing"); }
      
      //I'd rather have some lifecycle method that gets called on this class by the recovery manager
      //but there doesn't seem to be one.
      //Therefore the only place I can close the connection is in the finalizer
      if (conn != null)
      {
         try
         {
            conn.close();
         }
         catch (Exception ignore)
         {
            //Ignore
         }
      }
   }

}
