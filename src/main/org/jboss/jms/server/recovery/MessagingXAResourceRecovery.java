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
package org.jboss.jms.server.recovery;

import java.util.StringTokenizer;

import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;

import org.jboss.jms.jndi.JMSProviderAdapter;
import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * 
 * A XAResourceRecovery instance that can be used to recover any JMS provider.
 * 
 * 
 * This class will create a new XAConnection/XASession/XAResource on each sweep from the recovery manager.
 * 
 * It can probably be optimised to keep the same XAResource between sweeps and only recreate if
 * a problem with the connection to the provider is detected, but considering that typical sweep periods
 * are of the order of 10s of seconds to several minutes, then the extra complexity of the code required
 * for that does not seem to be a good tradeoff.
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
   
   private String providerAdaptorName;
   
   private JMSProviderAdapter providerAdaptor;

   private boolean hasMore;
   
   private String username;
   
   private String password;
   
   private XAConnection conn;
   
   private XAResource res;

   public MessagingXAResourceRecovery()
   {
      if(trace) log.trace("Constructing BridgeXAResourceRecovery");
   }

   public boolean initialise(String config)
   {
      if (log.isTraceEnabled()) { log.trace(this + " intialise: " + config); }
      
      StringTokenizer tok = new StringTokenizer(config, ",");
      
      //First (mandatory) param is the provider adaptor name
      
      if (!tok.hasMoreTokens())
      {
         throw new IllegalArgumentException("Must specify provider adaptor name in config");
      }
      
      providerAdaptorName = tok.nextToken();
      
      InitialContext ic = null;
      
      try
      {
         ic = new InitialContext();
         
         providerAdaptor = (JMSProviderAdapter)ic.lookup(providerAdaptorName);         
      }
      catch (Exception e)
      {
      	//Note - we only log at trace, since this is likely to happen on the first pass since, when
      	//deployed in JBAS the recovery manager will typically start up before the JMSProviderLoaders
         log.trace("Failed to look up provider adaptor", e);
         
         return false;
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
      
      //Next two (optional) parameters are the username and password to use for creating the connection
      //for recovery
      
      if (tok.hasMoreTokens())
      {
         username = tok.nextToken();
         
         if (!tok.hasMoreTokens())
         {
            throw new IllegalArgumentException("If username is specified, password must be specified too");
         }
         
         password = tok.nextToken();
      }
         
      hasMore = true;
      
      if (log.isTraceEnabled()) { log.trace(this + " initialised"); }      
      
      return true;      
   }

   public boolean hasMoreResources()
   {
      if (log.isTraceEnabled()) { log.trace(this + " hasMoreResources"); }
      
      if (providerAdaptor == null)
      {
         return false;
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
       * And we return a new XAResource every time it is called.
       * This makes this resilient to failure, since if the network fails
       * between the XAResource and it's server, on the next pass a new one will
       * be create and if the server is back up it will work.
       * This means there is no need for an XAResourceWrapper which is a technique used in the 
       * old JMSProviderXAResourceRecovery
       * The recovery manager will throw away the XAResource after every sweep.
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
         
         Context ic = null;
         
         try
         {
            ic = providerAdaptor.getInitialContext();
            
            Object obj = ic.lookup(providerAdaptor.getFactoryRef());
            
            if (!(obj instanceof XAConnectionFactory))
            {
               throw new IllegalArgumentException("Connection factory from jms provider is not a XAConnectionFactory");
            }
            
            XAConnectionFactory connectionFactory = (XAConnectionFactory)obj;
            
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

