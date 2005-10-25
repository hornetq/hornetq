/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;

import javax.jms.IllegalStateException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;


/**
 * An interceptor for checking closed state. It waits for other invocations to complete before
 * allowing the close. I.e. it performs the function of a "valve"
 * 
 * Important! There should be *one instance* of this interceptor per instance of Connection,
 * Session, MessageProducer, MessageConsumer or QueueBrowser
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a> Adapted from the JBoss 4 version
 */
public class ClosedInterceptor
   implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClosedInterceptor.class);
   
   private static final long serialVersionUID = 7564456983116742854L;

   
   /** Not closed */
   private static final int OPEN = 0;
   
   /** Performing the close */
   private static final int WAITING_TO_CLOSE = 1;
   
   /** Closed */
   private static final int CLOSED = 2;
   
   // Attributes ----------------------------------------------------

   /** The state of the object */
   private int state = OPEN;
   
   /** The inuse count */
   private int inuseCount = 0;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ClosedInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      String methodName = ((MethodInvocation) invocation).getMethod().getName();

      if (log.isTraceEnabled()) { log.trace(methodName + " " + ((JMSMethodInvocation)invocation).getHandler().getDelegateID()); }

      boolean isClose = methodName.equals("close");
      
      if (isClose)
      {
         waitForInvocationsToComplete();
      }
      else
      {
         inuse();
      }

      try
      {
         return invocation.invokeNext();
      }
      finally
      {
         if (isClose)
         {
            closed();
         }
         else
         {
            done();
         }
      }
   }

   // Protected ------------------------------------------------------

  

   /**
    * Wait for all invocations to complete
    * 
    * @return true when already closed
    */
   protected synchronized void waitForInvocationsToComplete()
      throws Throwable
   {
      state = WAITING_TO_CLOSE;
      while (inuseCount > 0)
      {
         wait();
      }
      
   }

   /**
    * Closed the object
    */
   protected synchronized void closed()
      throws Throwable
   {
      state = CLOSED;
      if (log.isTraceEnabled()) { log.trace("closed"); }
   }
   
   /**
    * Mark the object as inuse
    */
   protected synchronized void inuse()
      throws Throwable
   {
      if (state == CLOSED)
      {
         throw new IllegalStateException("The object is closed");
      }
      if (state == WAITING_TO_CLOSE)
      {
         throw new IllegalStateException("The object is waiting to close - will not accept any more invocations");
      }
      ++inuseCount;
   }
   
   /**
    * Mark the object as no longer inuse 
    */
   protected synchronized void done()
      throws Throwable
   {
      if (--inuseCount == 0)
      {
         notifyAll();
      }
   }

   

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   
   
   // Inner Classes --------------------------------------------------

}

