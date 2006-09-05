/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.List;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.XMLUtil;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.exchange.Binding;

/**
 * A deployable JBoss Messaging queue.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Queue extends DestinationServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public Queue()
   {
      super(false);
   }

   public Queue(boolean createProgrammatically)
   {
      super(createProgrammatically);
   }

   // JMX managed attributes ----------------------------------------
   
   public int getMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return 0;
         }
         
         Binding binding = (Binding)exchange.listBindingsForWildcard(name).get(0);
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue:" + name);
         }
         
         MessageQueue queue = binding.getQueue();
   
   	   return queue.messageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getMessageCount");
      }
   }

   // JMX managed operations ----------------------------------------
   
   public synchronized void startService() throws Exception
   {
      try
      {
         started = true;
   
         if (serviceName != null)
         {
            name = serviceName.getKeyProperty("name");
         }
   
         if (name == null || name.length() == 0)
         {
            throw new IllegalStateException( "The " + (isQueue() ? "queue" : "topic") + " " +
                                             "name was not properly set in the service's" +
                                             "ObjectName");
         }
   
         ServerPeer serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");

         dm = serverPeer.getDestinationManager();
         sm = serverPeer.getSecurityManager();         
         exchange = serverPeer.getDirectExchangeDelegate();
         
         //Binding must be added before destination is registered in JNDI
         //otherwise the user could get a reference to the destination and use it
         //while it is still being loaded
         
         MessageStore ms = serverPeer.getMessageStore();
         PersistenceManager pm = serverPeer.getPersistenceManagerDelegate(); 
         //Binding might already exist
         
         log.info("Deploying queue");
         
         Binding binding = exchange.getBindingForName(name);
         
         log.info("binding is: " + binding);
         
         if (binding != null)
         {
            //Reload the queue for the binding
            log.info("reloading queue");
            exchange.reloadQueues(name, ms, pm, fullSize, pageSize, downCacheSize);
            log.info("reloaded queue");
         }
         else
         {         
            //Make a binding for this queue
            log.info("bidning queue");
            exchange.bindQueue(name, name, null, false, true, ms, pm, fullSize, pageSize, downCacheSize);
            log.info("bound queue");
         }
         
         JBossQueue q = new JBossQueue(name, fullSize, pageSize, downCacheSize);
         
         jndiName = dm.registerDestination(q, jndiName, securityConfig);
        
         log.debug(this + " security configuration: " + (securityConfig == null ?
            "null" : "\n" + XMLUtil.elementToString(securityConfig)));

         log.info(this + " started, fullSize=" + fullSize + ", pageSize=" + pageSize + ", downCacheSize=" + downCacheSize);
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public void stopService() throws Exception
   {
      try
      {
         dm.unregisterDestination(new JBossQueue(name));
         
         //We undeploy the queue from memory - this also deactivates the binding
         exchange.unloadQueues(name);
         
         started = false;
         
         log.info(this + " stopped");
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }
   
   public void removeAllMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return;
         }
         
         Binding binding = (Binding)exchange.listBindingsForWildcard(name).get(0);
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue:" + name);
         }
         
         MessageQueue queue = binding.getQueue();
   
         queue.removeAllReferences();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " removeAllMessages");
      } 
   }
   
   public List listMessages(String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return new ArrayList();
         }
         
         if (selector != null)
         {
            selector = selector.trim();
            if (selector.equals(""))
            {
               selector = null;
            }
         }
         
         Binding binding = (Binding)exchange.listBindingsForWildcard(name).get(0);
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue:" + name);
         }
         
         MessageQueue queue = binding.getQueue();
   
         try 
         {
            List msgs;
            if (selector == null)
            {
               msgs = queue.browse();
            }
            else
            {
               msgs = queue.browse(new Selector(selector));
            }
            return msgs;
         }
         catch (InvalidSelectorException e)
         {
            Throwable th = new JMSException(e.getMessage());
            th.initCause(e);
            throw (JMSException)th;
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessages");
      } 
   }
    
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
