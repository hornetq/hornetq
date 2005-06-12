/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.Closeable;


/**
 * An interceptor for checking closed state. It waits for other invocations to complete before
 * allowing the close. I.e. it performs the function of a "valve"
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a> Adapted from the JBoss 4 version
 */
public class ClosedInterceptor
   implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 7564456983116742854L;

   
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
         inuse();
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
    * Check the closing notification has not already been done
    * 
    * @return true when already closing or closed
    */
   protected synchronized boolean checkClosingAlreadyDone()
      throws Throwable
   {
      if (state != NOT_CLOSED)
      {
         return true;
      }
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
      {
         return true;
      }
      while (inuseCount > 0)
      {
         wait();
      }
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
      {
         throw new IllegalStateException("Already closed");
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

   /**
    * Close children and remove from parent
    * 
    * @param invocation the invocation
    */
   protected void maintainRelatives(Invocation invocation)
   {                  
      
      //Get the InvocationHandler for this invocation
      JMSInvocationHandler thisHandler = ((JMSMethodInvocation)invocation).getHandler();
             
      //We use a clone to avoid a deadlock where requests
      //are made to close parent and child concurrently
      
      Set clone = null;
      Set children = thisHandler.getChildren();
      synchronized (children)
      {
         clone = new HashSet(children);
      }
      
      // Cycle through the children this will do a depth
      // first close
      for (Iterator i = clone.iterator(); i.hasNext();)
      {
         JMSInvocationHandler childHandler = (JMSInvocationHandler) i.next();
         
         Closeable child = (Closeable) childHandler.getDelegate();             
         try
         {
            child.closing();
            child.close();
         }
         catch (Throwable ignored)
         {
            // Add a log interceptor to the child if you want the error
            ignored.printStackTrace();
         }
      }
      
      // Remove from the parent
      JMSInvocationHandler parentHandler = thisHandler.getParent();
      if (parentHandler != null)
      {
         parentHandler.removeChild(thisHandler);
      }
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   
   
   // Inner Classes --------------------------------------------------

}

