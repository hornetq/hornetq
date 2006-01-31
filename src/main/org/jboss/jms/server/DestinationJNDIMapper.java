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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Manages JNDI mapping and delegates core destination state management to a CoreDestinationStore.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   protected CoreDestinationStore coreDestinationStore;

   // TODO synchronize access to these
   // <name - JNDI name>
   protected Map queueNameToJNDI;
   protected Map topicNameToJNDI;

   // Constructors --------------------------------------------------

   public DestinationJNDIMapper(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      coreDestinationStore = new CoreDestinationStore(this);

      queueNameToJNDI = new ConcurrentReaderHashMap();
      topicNameToJNDI = new ConcurrentReaderHashMap();
   }
   
   // DestinationManager implementation -----------------------------
   
   public String registerDestination(boolean isQueue, String name, String jndiName,
                                     Element securityConfiguration) throws JMSException
   {
      String parentContext;
      String jndiNameInContext;

      if (jndiName == null)
      {
         parentContext = isQueue ?
            serverPeer.getDefaultQueueJNDIContext() : serverPeer.getDefaultTopicJNDIContext();

         jndiNameInContext = name;
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
         throw new InvalidDestinationException("Destination " + name + " already exists");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
      catch(Exception e)
      {
         throw new JBossJMSException("JNDI failure", e);
      }

      Destination jmsDestination =
         isQueue ? (Destination) new JBossQueue(name) : (Destination) new JBossTopic(name);

      coreDestinationStore.createCoreDestination(jmsDestination);

      try
      {
         Context c = JNDIUtil.createContext(initialContext, parentContext);
         c.rebind(jndiNameInContext, jmsDestination);
         if (isQueue)
         {
            queueNameToJNDI.put(name, jndiName);
         }
         else
         {
            topicNameToJNDI.put(name, jndiName);
         }
      }
      catch(Exception e)
      {
         coreDestinationStore.destroyCoreDestination(isQueue, name);
         throw new JBossJMSException("JNDI failure", e);
      }

      // if the destination has no security configuration, then the security manager will always
      // use its current default security configuration when requested to authorize requests for
      // that destination
      if (securityConfiguration != null)
      {
         serverPeer.getSecurityManager().setSecurityConfig(isQueue, name, securityConfiguration);

      }

      log.debug((isQueue ? "queue" : "topic") + " " + name +
                " registered and bound in JNDI as " + jndiName );

      return jndiName;
   }

   public void unregisterDestination(boolean isQueue, String name) throws JMSException
   {
      coreDestinationStore.destroyCoreDestination(isQueue, name);

      String jndiName = null;
      if (isQueue)
      {
         jndiName = (String)queueNameToJNDI.remove(name);
      }
      else
      {
         jndiName = (String)topicNameToJNDI.remove(name);
      }
      if (jndiName == null)
      {
         return;
      }

      try
      {
         initialContext.unbind(jndiName);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("JNDI failure", e);
      }

      serverPeer.getSecurityManager().clearSecurityConfig(isQueue, name);
      //serverPeer.getDurableSubscriptionStoreDelegate().clearTopic();

      log.debug("unregistered " + (isQueue ? "queue " : "topic ") + name);
   }

   public synchronized void createTemporaryDestination(javax.jms.Destination jmsDestination)
      throws JMSException
   {
      coreDestinationStore.createCoreDestination(jmsDestination);
   }

   public void destroyTemporaryDestination(javax.jms.Destination jmsDestination)
   {
      JBossDestination d = (JBossDestination)jmsDestination;
      boolean isQueue = d.isQueue();
      String name = d.getName();
      coreDestinationStore.destroyCoreDestination(isQueue, name);
   }

   public CoreDestination getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      return coreDestinationStore.getCoreDestination(isQueue, name);
   }

   public CoreDestination getCoreDestination(javax.jms.Destination d) throws JMSException
   {
      boolean isQueue = d instanceof javax.jms.Queue;
      String name =
         isQueue ? ((javax.jms.Queue)d).getQueueName() : ((javax.jms.Topic)d).getTopicName();

      return getCoreDestination(isQueue, name);
   }

   // Public --------------------------------------------------------

   public boolean isDeployed(boolean isQueue, String name)
   {
      return isQueue ? queueNameToJNDI.containsKey(name) : topicNameToJNDI.containsKey(name);
   }

   public Set getDestinations()
   {
      Set destinations = Collections.EMPTY_SET;

      for(Iterator i = queueNameToJNDI.keySet().iterator(); i.hasNext(); )
      {
         if (destinations == Collections.EMPTY_SET)
         {
            destinations = new HashSet();
         }
         destinations.add(new JBossQueue((String)i.next()));
      }
      for(Iterator i = topicNameToJNDI.keySet().iterator(); i.hasNext(); )
      {
         if (destinations == Collections.EMPTY_SET)
         {
            destinations = new HashSet();
         }
         destinations.add(new JBossTopic((String)i.next()));
      }
      return destinations;
   }

   // Package protected ---------------------------------------------

   void start() throws Exception
   {
      coreDestinationStore.start();

      initialContext = new InitialContext();

      // see if the default queue/topic contexts are there, and if they're not, create them
      createContext(serverPeer.getDefaultQueueJNDIContext());
      createContext(serverPeer.getDefaultTopicJNDIContext());
   }

   void stop() throws Exception
   {
      // destroy all destinations
      for(Iterator i = queueNameToJNDI.keySet().iterator(); i.hasNext(); )
      {
         unregisterDestination(true, (String)i.next());
      }

      for(Iterator i = topicNameToJNDI.keySet().iterator(); i.hasNext(); )
      {
         unregisterDestination(false, (String)i.next());
      }

      initialContext.destroySubcontext(serverPeer.getDefaultQueueJNDIContext());
      initialContext.destroySubcontext(serverPeer.getDefaultTopicJNDIContext());

      initialContext.close();

      coreDestinationStore.stop();
   }

   ServerPeer getServerPeer()
   {
      return serverPeer;
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
            throw new JBossJMSException(contextName + " is already bound " +
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
