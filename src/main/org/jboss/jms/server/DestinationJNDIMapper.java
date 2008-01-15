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
import java.util.Map;

import javax.jms.InvalidDestinationException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.util.JNDIUtil;

/**
 * Keeps track of destinations - including temporary destinations
 * Also manages the mapping of non temporary destinations to JNDI context
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DestinationJNDIMapper implements DestinationManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DestinationJNDIMapper.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Context initialContext;

   // Map <name , destination holder>
   private Map queueMap;
   private Map topicMap;
   
   private MessagingServer messagingServer;
         
   // Constructors --------------------------------------------------

   public DestinationJNDIMapper(MessagingServer messagingServer) throws Exception
   {;
      queueMap = new HashMap();
      topicMap = new HashMap();
      this.messagingServer = messagingServer;
   }
   
   // DestinationManager implementation -----------------------------
   
   public synchronized void registerDestination(Destination destination, String jndiName) throws Exception
   {          
      if (!destination.isTemporary())
      {
         String parentContext;
         String jndiNameInContext;
            
         if (jndiName == null)
         {
            parentContext = destination.getType() == DestinationType.QUEUE ?
               messagingServer.getConfiguration().getDefaultQueueJNDIContext() : messagingServer.getConfiguration().getDefaultTopicJNDIContext();
   
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
         
         JBossDestination jbDest;
         
         if (destination.getType() == DestinationType.QUEUE)
         {
            jbDest = new JBossQueue(destination.getName());
         }
         else
         {
            jbDest = new JBossTopic(destination.getName());
         }
           
         c.rebind(jndiNameInContext, jbDest);         
      }
             
      log.debug((destination.getType() == DestinationType.QUEUE ? "queue" : "topic") + " " + destination.getName() + " registered ");
      log.debug((destination.getType() == DestinationType.QUEUE ? "queue" : "topic") + " bound in JNDI as " + jndiName);
   }

   public synchronized void unregisterDestination(Destination destination, String jndiName) throws Exception
   {      
      initialContext.unbind(jndiName);      
   
      messagingServer.getSecurityManager().clearSecurityConfig(destination.getType() == DestinationType.QUEUE, destination.getName());
            
      log.debug("unregistered " + (destination.getType() == DestinationType.QUEUE ? "queue " : "topic ") + destination.getName());
   }
   
  
   // MessagingComponent implementation -----------------------------
   
   public void start() throws Exception
   {
      initialContext = new InitialContext();

      // see if the default queue/topic contexts are there, and if they're not, create them
      createContext(messagingServer.getConfiguration().getDefaultQueueJNDIContext());
      createContext(messagingServer.getConfiguration().getDefaultTopicJNDIContext());
   }

   public void stop() throws Exception
   {      
      initialContext.unbind(messagingServer.getConfiguration().getDefaultQueueJNDIContext());
      initialContext.unbind(messagingServer.getConfiguration().getDefaultTopicJNDIContext());

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
