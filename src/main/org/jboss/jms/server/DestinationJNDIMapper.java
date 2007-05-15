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

import java.util.*;
import javax.jms.InvalidDestinationException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import org.jboss.jms.destination.*;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.messaging.util.JNDIUtil;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.logging.Logger;

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

   private Context initialContext;

   // Map <name , destination holder>
   private Map queueMap;
   private Map topicMap;
   
   private ServerPeer serverPeer;
         
   // Constructors --------------------------------------------------

   public DestinationJNDIMapper(ServerPeer serverPeer) throws Exception
   {
     // this.serverPeer = serverPeer;
      queueMap = new HashMap();
      topicMap = new HashMap();
      this.serverPeer = serverPeer;
   }
   
   // DestinationManager implementation -----------------------------
   
   public synchronized void registerDestination(ManagedDestination destination) throws Exception
   {          
      String jndiName = destination.getJndiName();
      
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
         
         destination.setJndiName(jndiName);
         
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
         
         JBossDestination jbDest;
         
         if (destination.isQueue())
         {
            if (destination.isTemporary())
            {
               jbDest = new JBossTemporaryQueue(destination.getName());
            }
            else
            {
               jbDest = new JBossQueue(destination.getName());
            }
         }
         else
         {
            if (destination.isTemporary())
            {
               jbDest = new JBossTemporaryTopic(destination.getName());
            }
            else
            {
               jbDest = new JBossTopic(destination.getName());
            }
         }
         
         c.rebind(jndiNameInContext, jbDest);         
      }
            
      if (destination.isQueue())
      {
         queueMap.put(destination.getName(), destination);
      }
      else
      {
         topicMap.put(destination.getName(), destination);
      }      
      
      log.debug((destination.isQueue() ? "queue" : "topic") + " " + destination.getName() + " registered ");
      log.debug((destination.isQueue() ? "queue" : "topic") + " bound in JNDI as " + jndiName);
   }

   public synchronized void unregisterDestination(ManagedDestination destination) throws Exception
   {
      String jndiName = null;
      if (destination.isQueue())
      {
         ManagedDestination dest = (ManagedDestination)queueMap.remove(destination.getName());
         if (dest != null)
         {
            jndiName = dest.getJndiName();
         }
      }
      else
      {
         ManagedDestination dest = (ManagedDestination)topicMap.remove(destination.getName());
         if (dest != null)
         {
            jndiName = dest.getJndiName();
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
   
   public synchronized ManagedDestination getDestination(String name, boolean isQueue)
   {
      Map m = isQueue ? queueMap : topicMap;
      
      ManagedDestination holder = (ManagedDestination)m.get(name);
      
      return holder;
   }
   
   public synchronized Set getDestinations()
   {
      Set destinations = new HashSet();
      
      Iterator iter = queueMap.values().iterator();
      while (iter.hasNext())
      {
         ManagedDestination dest = (ManagedDestination)iter.next();
         if (dest.isTemporary())
         {
            destinations.add(new JBossTemporaryQueue(dest.getName()));
         }
         else
         {
            destinations.add(new JBossQueue(dest.getName()));
         }
      }
      
      iter = topicMap.values().iterator();
      while (iter.hasNext())
      {
         ManagedDestination dest = (ManagedDestination)iter.next();
         if (dest.isTemporary())
         {
            destinations.add(new JBossTemporaryTopic(dest.getName()));
         }
         else
         {
            destinations.add(new JBossTopic(dest.getName()));
         }
      }
      
      return destinations;
   }
   
   // MessagingComponent implementation -----------------------------
   
   public void start() throws Exception
   {
      initialContext = new InitialContext();

      // see if the default queue/topic contexts are there, and if they're not, create them
      createContext(serverPeer.getDefaultQueueJNDIContext());
      createContext(serverPeer.getDefaultTopicJNDIContext());
   }

   public void stop() throws Exception
   {
      Set queues = new HashSet(queueMap.values());
      
      Set topics = new HashSet(topicMap.values());
      
      // remove all destinations from JNDI
      for(Iterator i = queues.iterator(); i.hasNext(); )
      {
         unregisterDestination((ManagedDestination)i.next());         
      }

      for(Iterator i = topics.iterator(); i.hasNext(); )
      {
         unregisterDestination((ManagedDestination)i.next());
      }

      initialContext.unbind(serverPeer.getDefaultQueueJNDIContext());
      initialContext.unbind(serverPeer.getDefaultTopicJNDIContext());

      initialContext.close();
   }
  
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
  
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
   
}
