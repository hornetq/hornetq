/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

/**
 * 
 * Handles stuff related to the producer
 * 
 * Important! There is one instance of this interceptor per instance of Producer
 * and Session
 *
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ProducerInterceptor implements Interceptor, Serializable
{   
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -5145024271247414244L;
   
   private static final Logger log = Logger.getLogger(ProducerInterceptor.class);
   
   // Attributes ----------------------------------------------------
   
   protected Destination defaultDestination;
   
   protected int defaultDeliveryMode = DeliveryMode.PERSISTENT;
   
   protected boolean disableMessageTimestamp = false;
   
   protected int defaultPriority = 4;
   
   protected long defaultTimeToLive = 0;
   
   protected boolean messageIDDisabled = false;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ----------------------------------
   
   public String getName()
   {
      return "ProducerInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {      
      if (!(invocation instanceof MethodInvocation))
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();
      Object[] args = mi.getArguments();
      
      if (log.isTraceEnabled()) { log.trace("handling " + methodName); }
      
      if ("createProducerDelegate".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("Creating producer delegate"); }
         ProducerDelegate producerDelegate = (ProducerDelegate)invocation.invokeNext();
         Destination theDest = ((Destination)mi.getArguments()[0]);
         producerDelegate.setDestination(theDest);
         return producerDelegate;
      }
      else if ("send".equals(methodName))
      {
         
         Destination destination = (Destination)args[0];
         Message m = (Message)args[1];
         int deliveryMode = ((Integer)args[2]).intValue();
         int priority = ((Integer)args[3]).intValue();
         long timeToLive = ((Long)args[4]).longValue();

         // configure the message for sending, using attributes stored as metadata

         String messageID = generateMessageID();
         m.setJMSMessageID(messageID);
         
         //ProducerState state = getProducerState(mi);

         if (deliveryMode == -1)
         {
            deliveryMode = defaultDeliveryMode;

            if (log.isTraceEnabled()) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
         }
         m.setJMSDeliveryMode(deliveryMode);

         if (priority == -1)
         {
            priority = defaultPriority;

            if (log.isTraceEnabled()) { log.trace("Using producer's default priority: " + priority); }
         }
         m.setJMSPriority(priority);

         if (disableMessageTimestamp)
         {
            m.setJMSTimestamp(0l);
         }
         else
         {
            m.setJMSTimestamp(System.currentTimeMillis());
         }

         if (timeToLive == Long.MIN_VALUE)
         {
            timeToLive = defaultTimeToLive;

            if (log.isTraceEnabled()) { log.trace("Using producer's default timeToLive: " + timeToLive); }
         }

         if (timeToLive == 0)
         {
            //Zero implies never expires
            m.setJMSExpiration(0);
         }
         else
         {
            m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
         }

         if (destination == null)
         {
            destination = defaultDestination;
            
            if (destination == null)
            {
               throw new UnsupportedOperationException("Destination not specified");
            }

            if (log.isTraceEnabled()) { log.trace("Using producer's default destination: " + destination); }
         }
         else
         {
            //If a default destination was already specified then this must be same destination as that
            //specified in the arguments
            //Destination defaultDestination = state.destination;
            
            if (defaultDestination != null)
            {
               if (!defaultDestination.equals(destination))
               {
                  throw new UnsupportedOperationException("Where a default destination is specified for the sender " +
                                                          "and a destination is specified in the arguments to the " +
                                                          "send, these destinations must be equal");
               }
            }
         }

         m.setJMSDestination(destination);
         
         // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
         // a message whose implementation is not one of its own.

         // JMS 1.1 Sect. 3.9: After sending a message, a client may retain and modify it without
         // affecting the message that has been sent. The same message object may be sent
         // multiple times.

         if (log.isTraceEnabled()) { log.trace("Copying message"); }
         
         JBossMessage copy = JBossMessage.copy(m);
                 
         if (log.isTraceEnabled()) { log.trace("Calling afterSend"); }
         copy.afterSend();                                  

         // send the copy down the stack
         args[1] = copy;
      }
      else if ("setDisableMessageID".equals(methodName))
      {
         messageIDDisabled = ((Boolean)args[0]).booleanValue();       
         return null;
      }
      else if ("getDisableMessageID".equals(methodName))
      {
         return messageIDDisabled ? Boolean.TRUE : Boolean.FALSE;         
      }
      else if ("setDisableMessageTimestamp".equals(methodName))
      {
         disableMessageTimestamp = ((Boolean)args[0]).booleanValue();
         return null;
      }
      else if ("getDisableMessageTimestamp".equals(methodName))
      {
         return disableMessageTimestamp ? Boolean.TRUE : Boolean.FALSE;         
      }
      else if ("setDeliveryMode".equals(methodName))
      {
         defaultDeliveryMode = ((Integer)args[0]).intValue();         
         return null;
      }
      else if ("getDeliveryMode".equals(methodName))
      {
         return new Integer(defaultDeliveryMode);        
      }
      else if ("setPriority".equals(methodName))
      {
         defaultPriority = ((Integer)args[0]).intValue();         
         return null;
      }
      else if ("getPriority".equals(methodName))
      {
         return new Integer(defaultPriority);        
      }
      else if ("setTimeToLive".equals(methodName))
      {
         defaultTimeToLive = ((Long)args[0]).longValue();         
         return null;
      }
      else if ("getTimeToLive".equals(methodName))
      {
         return new Long(defaultTimeToLive);        
      }
      else if ("getDestination".equals(methodName))
      {
         if (log.isTraceEnabled()) { log.trace("getDestination:" + defaultDestination); }
         return defaultDestination;
      }
      else if ("setDestination".equals(methodName))
      {
         Destination dest = (Destination)mi.getArguments()[0];

         if (log.isTraceEnabled()) { log.trace("setDestination:" + dest); } 
         this.defaultDestination = dest;
         return null;
      }
  
      return invocation.invokeNext();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   

   private String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      sb.append(new GUID().toString());
      return sb.toString();
   }
   
   // Inner Classes -------------------------------------------------
   
}

