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
package org.jboss.jms.client.container;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.IllegalStateException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.logging.Logger;


/**
 * An interceptor for checking closed state. It waits for other invocations to complete before
 * allowing the close. I.e. it performs the function of a "valve"
 * 
 * This interceptor is PER_INSTANCE.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class ClosedInterceptor  implements Interceptor
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClosedInterceptor.class);
   
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
   
   private boolean trace = log.isTraceEnabled();

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
      
      if (trace) { log.trace(this + " invoking " + methodName); }
      

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
      if (trace) { log.trace("closed"); }
   }
   
   /**
    * Mark the object as inuse
    */
   protected synchronized void inuse()
      throws Throwable
   {
      if (state == IN_CLOSE || state == CLOSED)
      {
         throw new IllegalStateException("The object is closed");
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
      HierarchicalState state = ((DelegateSupport)invocation.getTargetObject()).getState();
            
      //We use a clone to avoid a deadlock where requests
      //are made to close parent and child concurrently
      
      Set clone;

      Set children = state.getChildren();
      
      if (children == null)
      {
         if (trace) { log.trace("No children"); }
         return;
      }
      
      synchronized (children)
      {
         clone = new HashSet(children);
      }
      
      // Cycle through the children this will do a depth
      // first close
      for (Iterator i = clone.iterator(); i.hasNext();)
      {
         HierarchicalState child = (HierarchicalState)i.next();      
         Closeable del = (Closeable)child.getDelegate();
         try
         {
            del.closing();
            del.close();
         }
         catch (Throwable ignored)
         {
            // Add a log interceptor to the child if you want the error
            ignored.printStackTrace();
         }
      }
      
      // Remove from the parent
      HierarchicalState parent = state.getParent();
      if (parent != null)
      {         
         parent.getChildren().remove(state);
      }
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

 
   // Inner Classes --------------------------------------------------

}

