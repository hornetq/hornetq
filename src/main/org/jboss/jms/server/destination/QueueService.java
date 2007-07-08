/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.ArrayList;
import java.util.List;

import javax.jms.IllegalStateException;

import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.XMLUtil;

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
public class QueueService extends DestinationServiceSupport implements QueueMBean
{
   // Constants ------------------------------------------------------------------------------------
   
   private static final String QUEUE_MESSAGECOUNTER_PREFIX = "Queue.";

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------
   
   public QueueService()
   {
      destination = new ManagedQueue();      
   }

   public QueueService(boolean createdProgrammatically)
   {
      super(createdProgrammatically);
      
      destination = new ManagedQueue();      
   }
   
   // ServiceMBeanSupport overrides ----------------------------------------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {                           
         // Binding must be added before destination is registered in JNDI otherwise the user could
         // get a reference to the destination and use it while it is still being loaded. Also,
         // binding might already exist.
           
         PostOffice po = serverPeer.getPostOfficeInstance();
                           
         Binding binding = po.getBindingForQueueName(destination.getName());
         
         Queue queue;
         
         if (binding != null)
         {                     
         	queue = binding.queue;
         	
         	if (queue.isActive())
         	{
         		throw new IllegalStateException("Cannot deploy queue " + destination.getName() + " it is already deployed");
         	}
         	
            queue.setPagingParams(destination.getFullSize(),
                                  destination.getPageSize(),
                                  destination.getDownCacheSize());  
            
            queue.load();
               
            // Must be done after load
            queue.setMaxSize(destination.getMaxSize());
            
            queue.activate();           
         }
         else
         {           
            // Create a new queue

            JMSCondition queueCond = new JMSCondition(true, destination.getName());
            
            queue = new MessagingQueue(nodeId, destination.getName(),
            		                     serverPeer.getChannelIDManager().getID(),
                                       serverPeer.getMessageStore(), serverPeer.getPersistenceManagerInstance(),
                                       true,
                                       destination.getMaxSize(), null,
                                       destination.getFullSize(), destination.getPageSize(),
                                       destination.getDownCacheSize(), destination.isClustered(),
                                       serverPeer.getRecoverDeliveriesTimeout());
            po.addBinding(new Binding(queueCond, queue, false), false);         
            
            queue.activate();
         }
         
         ((ManagedQueue)destination).setQueue(queue);
         
         String counterName = QUEUE_MESSAGECOUNTER_PREFIX + destination.getName();
         
         int dayLimitToUse = destination.getMessageCounterHistoryDayLimit();
         if (dayLimitToUse == -1)
         {
            //Use override on server peer
            dayLimitToUse = serverPeer.getDefaultMessageCounterHistoryDayLimit();
         }
         
         MessageCounter counter =
            new MessageCounter(counterName, null, queue, false, false,
                               dayLimitToUse);
         
         ((ManagedQueue)destination).setMessageCounter(counter);
                  
         serverPeer.getMessageCounterManager().registerMessageCounter(counterName, counter);
                       
         serverPeer.getDestinationManager().registerDestination(destination);
        
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
      	serverPeer.getDestinationManager().unregisterDestination(destination);
      	
         Queue queue = ((ManagedQueue)destination).getQueue();
         
         String counterName = QUEUE_MESSAGECOUNTER_PREFIX + destination.getName();
                  
         MessageCounter counter = serverPeer.getMessageCounterManager().unregisterMessageCounter(counterName);
         
         if (counter == null)
         {
            throw new IllegalStateException("Cannot find counter to unregister " + counterName);
         }
         
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

   // JMX managed attributes -----------------------------------------------------------------------
   
   public int getMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped");
            return 0;
         }
         
         return ((ManagedQueue)destination).getMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getMessageCount");
      }
   }
   
   public int getScheduledMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped");
            return 0;
         }
         
         return ((ManagedQueue)destination).getScheduledMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getMessageCount");
      }
   }
   
   public MessageCounter getMessageCounter()
   {
      return ((ManagedQueue)destination).getMessageCounter();
   }
   
   public MessageStatistics getMessageStatistics() throws Exception
   {
      List counters = new ArrayList();
      counters.add(getMessageCounter());
      
      List stats = MessageCounter.getMessageStatistics(counters);
      
      return (MessageStatistics)stats.get(0);
   }
   
   public String listMessageCounterAsHTML()
   {
      return super.listMessageCounterAsHTML(new MessageCounter[] { getMessageCounter() });
   }
   
   public int getConsumerCount() throws Exception
   {
      return ((ManagedQueue)destination).getConsumersCount();
   }
     
   // JMX managed operations -----------------------------------------------------------------------
      
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
   
   public List listAllMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listAllMessages(null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listAllMessages");
      } 
   }
   
   public List listAllMessages(String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listAllMessages(selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listAllMessages");
      } 
   }
   
   public List listDurableMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listDurableMessages(null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableMessages");
      } 
   }
   
   public List listDurableMessages(String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listDurableMessages(selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableMessages");
      } 
   }
   
   public List listNonDurableMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listNonDurableMessages(null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableMessages");
      } 
   }
   
   public List listNonDurableMessages(String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Queue is stopped.");
            return null;
         }
         
         return ((ManagedQueue)destination).listNonDurableMessages(selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableMessages");
      } 
   }
            
   public void resetMessageCounter()
   {
      ((ManagedQueue)destination).getMessageCounter().resetCounter();
   }
   
   public String listMessageCounterHistoryAsHTML()
   {
      return super.listMessageCounterHistoryAsHTML(new MessageCounter[] { getMessageCounter() });
   }
 
   public void resetMessageCounterHistory()
   {
      ((ManagedQueue)destination).getMessageCounter().resetHistory();
   }
       
   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
