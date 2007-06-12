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

import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

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
   
   private String providerAdaptorName;
   
   private boolean hasMore;
   
   private String username;
   
   private String password;
   
   private MessagingXAResourceWrapper res;

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
         
      res = new MessagingXAResourceWrapper(providerAdaptorName, username, password);
             
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
   
   protected void finalize()
   {
      res.close();  
   }
}

