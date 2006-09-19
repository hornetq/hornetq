/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.XMLUtil;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * MBean wrapper around a ManagedQueue
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueService extends DestinationServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public QueueService()
   {
      destination = new ManagedQueue();      
   }

   public QueueService(boolean createdProgrammatically)
   {
      super(createdProgrammatically);
      
      destination = new ManagedQueue();      
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
         
         return ((ManagedQueue)destination).getMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getMessageCount");
      }
   }

   // JMX managed operations ----------------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {                           
         postOffice = serverPeer.getQueuePostOfficeInstance();
         
         destination.setPostOffice(postOffice);

         //Binding must be added before destination is registered in JNDI
         //otherwise the user could get a reference to the destination and use it
         //while it is still being loaded
         
         //Binding might already exist
            
         Binding binding = postOffice.getBindingForQueueName(destination.getName());
         
         if (binding != null)
         {    
            PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();
            
            queue.setPagingParams(destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());
            queue.load();
            queue.activate();
         }
         else
         {         
            QueuedExecutor executor = (QueuedExecutor)pool.get();
            
            //Create a new queue       
            
            PagingFilteredQueue queue;
            
            if (postOffice.isLocal())
            {
               queue = new PagingFilteredQueue(destination.getName(), idm.getId(), ms, pm, true, true,                        
                                               executor, null,
                                               destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());
               
               postOffice.bindQueue(destination.getName(), queue);
            }
            else
            {
               queue = new LocalClusteredQueue(nodeId, destination.getName(), idm.getId(), ms, pm, true, true,                        
                                               executor, null,
                                               destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());
               
               ((ClusteredPostOffice)postOffice).bindClusteredQueue(destination.getName(), (LocalClusteredQueue)queue);
            }                        
         }
         
         //push security update to the server
         sm.setSecurityConfig(isQueue(), destination.getName(), destination.getSecurityConfig());
          
         dm.registerDestination(destination);
        
         log.debug(this + " security configuration: " + (destination.getSecurityConfig() == null ?
            "null" : "\n" + XMLUtil.elementToString(destination.getSecurityConfig())));
         
         started = true;         

         log.info(this + " started, fullSize=" + destination.getFullSize() +
                  ", pageSize=" + destination.getPageSize() + ", downCacheSize=" + destination.getDownCacheSize());
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public synchronized void stopService() throws Exception
   {
      try
      {
         dm.unregisterDestination(destination);
         
         //We undeploy the queue from memory - this also deactivates the binding
         Binding binding = postOffice.getBindingForQueueName(destination.getName());
         
         PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();
         
         queue.deactivate();
         queue.unload();
         
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
         
         ((ManagedQueue)destination).removeAllMessages();
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
         
         return ((ManagedQueue)destination).listMessages(selector);
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
