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
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Manages destinations. Manages JNDI mapping and delegates core destination management to a
 * CoreDestinationManager.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DestinationManagerImpl implements DestinationManagerImplMBean
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DestinationManagerImpl.class);
   
   public static final String DEFAULT_QUEUE_CONTEXT = "/queue";
   public static final String DEFAULT_TOPIC_CONTEXT = "/topic";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Context initialContext;
   protected CoreDestinationManager coreDestinationManager;

   // < name - JNDI name>
   protected Map queueNameToJNDI;
   protected Map topicNameToJNDI;


   // Constructors --------------------------------------------------

   public DestinationManagerImpl(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      //TODO close this inital context when DestinationManager stops
      initialContext = new InitialContext();
      coreDestinationManager = new CoreDestinationManager(this);
      queueNameToJNDI = new ConcurrentReaderHashMap();
      topicNameToJNDI = new ConcurrentReaderHashMap();
   }

   // DestinationManager implementation -----------------------------


   public String createQueue(String name) throws Exception
   {
      return createDestination(true, name, null);
   }

   public String createQueue(String name, String jndiName) throws Exception
   {
      return createDestination(true, name, jndiName);
   }

   public void destroyQueue(String name) throws Exception
   {
      removeDestination(true, name);
   }

   public String createTopic(String name) throws Exception
   {
      return createDestination(false, name, null);
   }

   public String createTopic(String name, String jndiName) throws Exception
   {
      return createDestination(false, name, jndiName);
   }

   public void destroyTopic(String name) throws Exception
   {
      removeDestination(false, name);
   }

   // Public --------------------------------------------------------

   ServerPeer getServerPeer()
   {
      return serverPeer;
   }
   
   public void destroyAllDestinations() throws Exception
   {
      Iterator iter = queueNameToJNDI.keySet().iterator();
      while (iter.hasNext())
      {
         String queueName = (String)iter.next();
         destroyQueue(queueName);
      }
      iter = topicNameToJNDI.keySet().iterator();
      while (iter.hasNext())
      {
         String topicName = (String)iter.next();
         destroyTopic(topicName);
      }
   }
   

   public synchronized void addTemporaryDestination(Destination jmsDestination) throws JMSException
   {
      coreDestinationManager.addCoreDestination(jmsDestination);
   }

   public void removeTemporaryDestination(Destination jmsDestination)
   {
      JBossDestination d = (JBossDestination)jmsDestination;
      boolean isQueue = d.isQueue();
      String name = d.getName();
      coreDestinationManager.removeCoreDestination(isQueue, name);
   }

   public CoreDestination getCoreDestination(Destination jmsDestination) throws JMSException
   {
      JBossDestination d = (JBossDestination)jmsDestination;
      boolean isQueue = d.isQueue();
      String name = d.getName();
      return getCoreDestination(isQueue, name);
   }

   public CoreDestination getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      return coreDestinationManager.getCoreDestination(isQueue, name);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String createDestination(boolean isQueue, String name, String jndiName) throws Exception
   {
      String parentContext;
      String jndiNameInContext;

      if (jndiName == null)
      {
         parentContext = (isQueue ? DEFAULT_QUEUE_CONTEXT : DEFAULT_TOPIC_CONTEXT);
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
         
         // Already exists - remove it - this allows the dest to be updated with any new values
         //this.removeDestination(isQueue, name);
         throw new InvalidDestinationException("Destination " + name + " already exists");
            
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      Destination jmsDestination =
         isQueue ? (Destination) new JBossQueue(name) : (Destination) new JBossTopic(name);

      coreDestinationManager.addCoreDestination(jmsDestination);

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
         coreDestinationManager.removeCoreDestination(isQueue, name);
         throw e;
      }

      log.debug((isQueue ? "Queue" : "Topic") + " " + name +
                " created and bound in JNDI as " + jndiName );

      return jndiName;
   }


   private void removeDestination(boolean isQueue, String name) throws Exception
   {

      coreDestinationManager.removeCoreDestination(isQueue, name);
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
      initialContext.unbind(jndiName);
   }

   // Inner classes -------------------------------------------------
}
