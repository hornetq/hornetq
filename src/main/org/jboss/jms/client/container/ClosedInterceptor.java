/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;

/**
 * An interceptor for checking closed state. It waits for
 * other invocations to complete allowing the close.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ClosedInterceptor
   implements Interceptor
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
      String methodName = ((MethodInvocation) invocation).method.getName();
      boolean isClosing = methodName.equals("closing");
      boolean isClose = methodName.equals("close");

      if (isClosing)
      {
         if (checkClosingAlreadyDone())
            return null;
      }
      else if (isClose)
      {
         if (checkCloseAlreadyDone())
            return null;
      }
      else
         inuse();

      try
      {
         return invocation.invokeNext();
      }
      finally
      {
         if (isClosing)
            closing();
         else if (isClose)
            closed();
         else
            done();
      }
   }

   // Protected ------------------------------------------------------

   /**
    * Check the closing notification has not already been done
    * 
    * @return true when already closing or closed
    */
   protected synchronized boolean checkClosingAlreadyDone()
      throws Throwable
   {
      if (state != NOT_CLOSED)
         return true;
      state = IN_CLOSING;
      return false;
   }

   /**
    * Closing the object
    */
   protected synchronized void closing()
      throws Throwable
   {
      state = CLOSING;
   }

   /**
    * Check the close has not already been done and
    * wait for all invocations to complete
    * 
    * @return true when already closed
    */
   protected synchronized boolean checkCloseAlreadyDone()
      throws Throwable
   {
      if (state != CLOSING)
         return true;
      while (inuseCount > 0)
         wait();
      state = IN_CLOSE;
      return false;
   }

   /**
    * Closed the object
    */
   protected synchronized void closed()
      throws Throwable
   {
      state = CLOSED;
   }
   
   /**
    * Mark the object as inuse
    */
   protected synchronized void inuse()
      throws Throwable
   {
      if (state != NOT_CLOSED)
         throw new IllegalStateException("Already closed");
      ++inuseCount;
   }
   
   /**
    * Mark the object as no longer inuse 
    */
   protected synchronized void done()
      throws Throwable
   {
      if (--inuseCount == 0)
         notifyAll();
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
