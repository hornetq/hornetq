/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.container;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.messaging.jms.container.Container;
import org.jboss.messaging.jms.client.Lifecycle;

/**
 * An interceptor for checking closed state. It waits for other invocations to complete allowing
 * the close.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ClosedInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   /** Not closed */
   private static final int NOT_CLOSED = 0;
   
   /** Closing */
   private static final int IN_CLOSING = 1;
   
   /** Closing */
   private static final int CLOSING = 2;

   /** Performing the close */
   private static final int IN_CLOSE = 3;
   
   /** Closed */
   private static final int CLOSED = -1;

   // Attributes ----------------------------------------------------

   /** The state of the object */
   private int state = NOT_CLOSED;
   
   /** The inUse count */
   private int inUseCount = 0;

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
      boolean isClosing = methodName.equals("closing");
      boolean isClose = methodName.equals("close");

      if (isClosing)
      {
         if (checkClosingAlreadyDone())
         {
            return null;
         }
      }
      else if (isClose)
      {
         if(checkCloseAlreadyDone())
         {
            return null;
         }
      }
      else
      {
         inUse();
      }

      if (isClosing)
      {
         maintainRelatives(invocation);
      }

      try
      {
         return invocation.invokeNext();
      }
      finally
      {
         if (isClosing)
         {
            closing();
         }
         else if (isClose)
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
    * Check the closing notification has not already been done.
    * 
    * @return true when already closing or closed.
    */
   protected synchronized boolean checkClosingAlreadyDone() throws Throwable
   {
      if (state != NOT_CLOSED)
      {
         return true;
      }
      state = IN_CLOSING;
      return false;
   }

   /**
    * Closing the object.
    */
   protected synchronized void closing() throws Throwable
   {
      state = CLOSING;
   }

   /**
    * Check the close has not already been done and wait for all invocations to complete.
    * 
    * @return true when already closed
    */
   protected synchronized boolean checkCloseAlreadyDone() throws Throwable
   {
      if (state != CLOSING)
      {
         return true;
      }
      while (inUseCount > 0)
      {
         wait();
      }
      state = IN_CLOSE;
      return false;
   }

   /**
    * Closed the object.
    */
   protected synchronized void closed() throws Throwable
   {
      state = CLOSED;
   }
   
   /**
    * Mark the object as inUse.
    */
   protected synchronized void inUse() throws Throwable
   {
      if (state != NOT_CLOSED)
      {
         throw new IllegalStateException("Already closed");
      }
      ++inUseCount;
   }
   
   /**
    * Mark the object as no longer inUse.
    */
   protected synchronized void done() throws Throwable
   {
      if (--inUseCount == 0)
      {
         notifyAll();
      }
   }

   /**
    * Close children and remove from parent.
    * 
    * @param invocation the invocation.
    */
   protected void maintainRelatives(Invocation invocation)
   {
      // We use a clone to avoid a deadlock where requests
      // are made to close parent and child concurrently
      Container container = Container.getContainer(invocation);
      Set clone = null;
      Set children = container.getChildren();
      synchronized (children)
      {
         clone = new HashSet(children);
      }
      
      // Cycle through the children this will do a depth first close
      for (Iterator i = clone.iterator(); i.hasNext();)
      {
         Container childContainer = (Container)i.next();
         Lifecycle child = (Lifecycle)childContainer.getProxy();
         try
         {
            child.closing();
            child.close();
         }
         catch (Throwable ignored)
         {
            // Add a log interceptor to the child if you want the error
         }
      }
      
      // Remove from the parent
      Container parent = container.getParent();
      if (parent != null)
      {
         parent.removeChild(container);
      }
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
