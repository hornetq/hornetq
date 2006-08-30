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
package org.jboss.jms.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.InvalidDestinationException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.logging.Logger;
import org.w3c.dom.Element;

/**
 * Keeps track of destinations - including temporary destinations
 * Also manages the mapping of non temporary destinations to JNDI context
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class DestinationJNDIMapper implements DestinationManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DestinationJNDIMapper.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Context initialContext;

   // Map <name , destination holder>
   protected Map queueMap;
   protected Map topicMap;
      
   // Constructors --------------------------------------------------

   public DestinationJNDIMapper(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      queueMap = new HashMap();
      topicMap = new HashMap();
   }
   
   // DestinationManager implementation -----------------------------
   
   public synchronized String registerDestination(JBossDestination destination, String jndiName,
                                                  Element securityConfiguration) throws Exception
   {            
      if (!destination.isTemporary())
      {
         String parentContext;
         String jndiNameInContext;
   
         if (jndiName == null)
         {
            parentContext = destination.isQueue() ?
               serverPeer.getDefaultQueueJNDIContext() : serverPeer.getDefaultTopicJNDIContext();
   
            jndiNameInContext = destination.getName();
            jndiName = parentContext + "/" + jndiNameInContext;
         }
         else
         {
            // TODO more solid parsing + test cases
            int sepIndex = jndiName.lastIndexOf('/');
            if (sepIndex == -1)
            {
               parentContext = "";
            }
            else
            {
               parentContext = jndiName.substring(0, sepIndex);
            }
            jndiNameInContext = jndiName.substring(sepIndex + 1);
         }
   
         try
         {
            initialContext.lookup(jndiName);
            throw new InvalidDestinationException("Destination " + destination.getName() + " already exists");
         }
         catch(NameNotFoundException e)
         {
            // OK
         }      

         Context c = JNDIUtil.createContext(initialContext, parentContext);
         
         c.rebind(jndiNameInContext, destination);
         
         //if the destination has no security configuration, then the security manager will always
         // use its current default security configuration when requested to authorize requests for
         // that destination
         if (securityConfiguration != null)
         {
            serverPeer.getSecurityManager().setSecurityConfig(destination.isQueue(),
                                                              destination.getName(),
                                                              securityConfiguration);
         }
      }
            
      if (destination.isQueue())
      {
         queueMap.put(destination.getName(), new DestinationHolder(destination, jndiName));
      }
      else
      {
         topicMap.put(destination.getName(), new DestinationHolder(destination, jndiName));
      }      
      
      log.debug((destination.isQueue() ? "queue" : "topic") + " " + destination.getName() + " registered ");
      log.debug((destination.isQueue() ? "queue" : "topic") + " bound in JNDI as " + jndiName);
      
      return jndiName;
   }

   public synchronized void unregisterDestination(JBossDestination destination) throws Exception
   {
      String jndiName = null;
      if (destination.isQueue())
      {
         DestinationHolder holder = (DestinationHolder)queueMap.remove(destination.getName());
         if (holder != null)
         {
            jndiName = holder.jndiName;
         }
      }
      else
      {
         DestinationHolder holder = (DestinationHolder)topicMap.remove(destination.getName());
         if (holder != null)
         {
            jndiName = holder.jndiName;
         }
      }
      if (jndiName == null)
      {
         return;
      }

      if (!destination.isTemporary())
      {
         initialContext.unbind(jndiName);      
   
         serverPeer.getSecurityManager().clearSecurityConfig(destination.isQueue(), destination.getName());
      }
      
      log.debug("unregistered " + (destination.isQueue() ? "queue " : "topic ") + destination.getName());
   }
   
   public synchronized boolean destinationExists(JBossDestination dest)
   {
      return getDestination(dest) != null;
   }
   
   public synchronized JBossDestination getDestination(JBossDestination dest)
   {
      Map m = dest.isTopic() ? topicMap : queueMap;
      
      DestinationHolder holder = (DestinationHolder)m.get(dest.getName());
      
      return holder == null ? null : holder.destination;
   }
   
   public synchronized Set getDestinations()
   {
      Set destinations = new HashSet();
      
      for(Iterator i = queueMap.values().iterator(); i.hasNext(); )
      {
         DestinationHolder holder = (DestinationHolder)i.next();
         destinations.add(holder.destination);
      }
      for(Iterator i = topicMap.values().iterator(); i.hasNext(); )
      {
         DestinationHolder holder = (DestinationHolder)i.next();
         destinations.add(holder.destination);
      }
      return destinations;
   }
   

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   void start() throws Exception
   {
      initialContext = new InitialContext();

      // see if the default queue/topic contexts are there, and if they're not, create them
      createContext(serverPeer.getDefaultQueueJNDIContext());
      createContext(serverPeer.getDefaultTopicJNDIContext());
   }

   void stop() throws Exception
   {
      Set queues = new HashSet(queueMap.keySet());
      
      Set topics = new HashSet(topicMap.keySet());
      
      // remove all destinations from JNDI
      for(Iterator i = queues.iterator(); i.hasNext(); )
      {
         unregisterDestination(new JBossQueue((String)i.next()));         
      }

      for(Iterator i = topics.iterator(); i.hasNext(); )
      {
         unregisterDestination(new JBossTopic((String)i.next()));
      }

      initialContext.unbind(serverPeer.getDefaultQueueJNDIContext());
      initialContext.unbind(serverPeer.getDefaultTopicJNDIContext());

      initialContext.close();
   }


   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void createContext(String contextName) throws Exception
   {
      Object context = null;
      try
      {
         context = initialContext.lookup(contextName);

         if (!(context instanceof Context))
         {
            throw new MessagingJMSException(contextName + " is already bound " +
                                        " and is not a JNDI context!");
         }
      }
      catch(NameNotFoundException e)
      {
         initialContext.createSubcontext(contextName);
         log.debug(contextName + " subcontext created");
      }
   }

   // Inner classes -------------------------------------------------
   
   private class DestinationHolder
   {
      DestinationHolder(JBossDestination destination, String jndiName)
      {
         this.destination = destination;
         this.jndiName = jndiName;
      }
      
      JBossDestination destination;
      
      String jndiName;
   }
}
