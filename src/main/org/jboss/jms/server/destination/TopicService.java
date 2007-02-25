/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;

import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.MessageQueueNameHelper;
import org.jboss.jms.util.XMLUtil;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;

/**
 * A deployable JBoss Messaging topic.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TopicService extends DestinationServiceSupport implements TopicMBean
{
   // Constants -----------------------------------------------------
   
   public static final String SUBSCRIPTION_MESSAGECOUNTER_PREFIX = "Subscription.";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicService()
   {
      destination = new ManagedTopic();      
   }
   
   public TopicService(boolean createdProgrammatically)
   {
      super(createdProgrammatically);
      
      destination = new ManagedTopic();      
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {
         postOffice = serverPeer.getPostOfficeInstance();

         if (postOffice == null)
          throw new IllegalArgumentException("Post Office instance not found. Check your destination configuration.");
         destination.setServerPeer(serverPeer);
         
         JMSCondition topicCond = new JMSCondition(false, destination.getName());
                    
         // We deploy any queues corresponding to pre-existing durable subscriptions
         Collection bindings = postOffice.getBindingsForCondition(topicCond);
         Iterator iter = bindings.iterator();
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
            
            PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();
                     
            //TODO We need to set the paging params this way since the post office doesn't store them
            //instead we should never create queues inside the postoffice - only do it at deploy time
            queue.setPagingParams(destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());
            
            queue.load();
                        
            queue.activate();  
               
            //Must be done after load
            queue.setMaxSize(destination.getMaxSize());  
            
            //Create a counter
            String counterName = SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();
            
            String subName = MessageQueueNameHelper.createHelper(queue.getName()).getSubName();
            
            int dayLimitToUse = destination.getMessageCounterHistoryDayLimit();
            if (dayLimitToUse == -1)
            {
               //Use override on server peer
               dayLimitToUse = serverPeer.getDefaultMessageCounterHistoryDayLimit();
            }
            
            MessageCounter counter =
               new MessageCounter(counterName, subName, queue, true, true,
                                  dayLimitToUse);
            
            serverPeer.getMessageCounterManager().registerMessageCounter(counterName, counter);            
         }

         dm.registerDestination(destination);
         
         log.debug(this + " security configuration: " + (destination.getSecurityConfig() == null ?
            "null" : "\n" + XMLUtil.elementToString(destination.getSecurityConfig())));
         
         started = true;

         log.info(this + " started, fullSize=" + destination.getFullSize() + ", pageSize=" + destination.getPageSize() + ", downCacheSize=" + destination.getDownCacheSize());
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
         
         //When undeploying a topic, any non durable subscriptions will be removed
         //Any durable subscriptions will survive in persistent storage, but be removed
         //from memory
         
         //First we remove any data for a non durable sub - a non durable sub might have data in the
         //database since it might have paged
         
         JMSCondition topicCond = new JMSCondition(false, destination.getName());         
         
         Collection bindings = postOffice.getBindingsForCondition(topicCond);
         
         Iterator iter = bindings.iterator();
         while (iter.hasNext())            
         {
            Binding binding = (Binding)iter.next();
            
            PagingFilteredQueue queue = (PagingFilteredQueue)binding.getQueue();
            
            if (!queue.isRecoverable())
            {
               queue.removeAllReferences();
               
               // Unbind
               if (!queue.isClustered())
               {
                  postOffice.unbindQueue(queue.getName());
               }
               else
               {
                  ((ClusteredPostOffice)postOffice).unbindClusteredQueue(queue.getName());
               }
            }
                        
            queue.deactivate();
            queue.unload();
            
            //unregister counter
            String counterName = SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();
            
            serverPeer.getMessageCounterManager().unregisterMessageCounter(counterName);                        
         }
          
         started = false;
         log.info(this + " stopped");
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }

   // JMX managed attributes ----------------------------------------
   
   public int getAllMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return -1;
         }
         
         return ((ManagedTopic)destination).getAllMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getAllMessageCount");
      } 
   }
   
   
   public int getDurableMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return -1;
         }
         
         return ((ManagedTopic)destination).getDurableMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getDurableMessageCount");
      }
   }
   
   public int getNonDurableMessageCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return -1;
         }
         
         return ((ManagedTopic)destination).getNonDurableMessageCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getNonDurableMessageCount");
      }
   }
   
   /**
    * All subscription count
    * @return all subscription count
    * @throws JMSException
    */
   public int getAllSubscriptionsCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return 0;
         }
   
         return ((ManagedTopic)destination).getAllSubscriptionsCount();        
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getAllSubscriptionsCount");
      } 
   }

   public int getDurableSubscriptionsCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return 0;
         }
         
         return ((ManagedTopic)destination).getDurableSubscriptionsCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getDurableSubscriptionsCount");
      } 
   }
   
   public int getNonDurableSubscriptionsCount() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return 0;
         }
         
         return ((ManagedTopic)destination).getNonDurableSubscriptionsCount();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getNonDurableSubscriptionsCount");
      } 
   }

   // JMX managed operations ----------------------------------------
      

   /**
    * Remove all messages from subscription's storage.
    */
   public void removeAllMessages() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return;
         }
         
         ((ManagedTopic)destination).removeAllMessages();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " removeAllMessages");
      } 
   }         
   
   public List listAllSubscriptions() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
         
         return ((ManagedTopic)destination).listAllSubscriptions();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listAllSubscriptions");
      } 
   }
   
   public List listDurableSubscriptions() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
         
         return ((ManagedTopic)destination).listDurableSubscriptions();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableSubscriptions");
      } 
   }
   
   public List listNonDurableSubscriptions() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
         
         return ((ManagedTopic)destination).listNonDurableSubscriptions();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableSubscriptions");
      } 
   }
   
   public String listAllSubscriptionsAsHTML() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return "";
         }
   
         return ((ManagedTopic)destination).listAllSubscriptionsAsHTML();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listAllSubscriptionsAsHTML");
      } 
   }
   
   public String listDurableSubscriptionsAsHTML() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return "";
         }
   
         return ((ManagedTopic)destination).listDurableSubscriptionsAsHTML();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableSubscriptionsAsHTML");
      } 
   }
   
   public String listNonDurableSubscriptionsAsHTML() throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return "";
         }
   
         return ((ManagedTopic)destination).listNonDurableSubscriptionsAsHTML();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableSubscriptionsAsHTML");
      } 
   }
   
   public List listAllMessages(String subscriptionId) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listAllMessages(subscriptionId, null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessages");
      }
   }
   
   public List listAllMessages(String subscriptionId, String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listAllMessages(subscriptionId, selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessages");
      }
   }
   
   
   public List listDurableMessages(String subscriptionId) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listDurableMessages(subscriptionId, null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableMessages");
      }
   }
   
   public List listDurableMessages(String subscriptionId, String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listDurableMessages(subscriptionId, selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listDurableMessages");
      }
   }
   
   public List listNonDurableMessages(String subscriptionId) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listNonDurableMessages(subscriptionId, null);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableMessages");
      }
   }
   
   public List listNonDurableMessages(String subscriptionId, String selector) throws Exception
   {
      try
      {
         if (!started)
         {
            log.warn("Topic is stopped.");
            return null;
         }
   
         return ((ManagedTopic)destination).listNonDurableMessages(subscriptionId, selector);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listNonDurableMessages");
      }
   }
   
   public List getMessageCounters()
      throws Exception
   {
      try
      {
         return ((ManagedTopic)destination).getMessageCounters();       
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " listMessagesNonDurableSub");
      } 
   }
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return false;
   }   

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
