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
 * allowing the close. I.e. it performs the function of a "valve".
 * 
 * This interceptor is PER_INSTANCE.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com>Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class ClosedInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClosedInterceptor.class);

   private static final int NOT_CLOSED = 0;
   private static final int IN_CLOSING = 1;
   private static final int CLOSING = 2;
   private static final int IN_CLOSE = 3; // performing the close
   private static final int CLOSED = -1;

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // The current state of the object guarded by this interceptor
   private int state = NOT_CLOSED;

   // The inuse count
   private int inUseCount;

   // The identity of the delegate this interceptor is associated with
   private DelegateIdentity id;

   // Static --------------------------------------------------------

   public static String stateToString(int state)
   {
      return state == NOT_CLOSED ? "NOT_CLOSED" :
         state == IN_CLOSING ? "IN_CLOSING" :
            state == CLOSING ? "CLOSING" :
               state == IN_CLOSE ? "IN_CLOSE" :
                  state == CLOSED ? "CLOSED" : "UNKNOWN";
   }

   // Constructors --------------------------------------------------

   public ClosedInterceptor()
   {
      state = NOT_CLOSED;
      inUseCount = 0;
      id = null;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ClosedInterceptor.");
      if (id == null)
      {
         sb.append("UNINITIALIZED");
      }
      else
      {
         sb.append(id.getType()).append("[").append(id.getID()).append("]");
      }
      return sb.toString();
   }

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ClosedInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      // maintain the identity of the delegate that sends invocation through this interceptor, for
      // logging purposes. It makes sense, since it's an PER_INSTANCE interceptor
      if (id == null)
      {
         id = DelegateIdentity.getIdentity(invocation);
      }

      String methodName = ((MethodInvocation)invocation).getMethod().getName();

      boolean isClosing = methodName.equals("closing");
      boolean isClose = methodName.equals("close");

      if (isClosing)
      {
         if (checkClosingAlreadyDone())
         {
            return new Long(-1);
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
         synchronized(this)
         {
            // object "in use", increment inUseCount
            if (state == IN_CLOSE || state == CLOSED)
            {
               log.error(this + ": method " + methodName + "() did not go through, " +
                                "the interceptor is " + stateToString(state));

               throw new IllegalStateException("The object is closed");
            }
            ++inUseCount;
         }
      }

      if (isClosing)
      {
         maintainRelatives(invocation);
      }

      try
      {
         return invocation.invokeNext();
      }
      catch (Exception t)
      {
      	if (isClosing || isClose)
      	{
	      	//We swallow exceptions in close/closing, this is because if the connection fails, it is naturally for code to then close
	      	//in a finally block, it would not then be appropriate to throw an exception. This is a common technique
	      	//Close should ALWAYS (well apart from Errors) succeed irrespective of whether the actual connection to the server is alive.
	      	return new Long(-1);
      	}
      	throw t;
      }
      finally
      {
         if (isClosing)
         {
            // We make sure we remove ourself AFTER the invocation has been made otherwise in a
            // failover situation we would end up divorced from the hierarchy and failover will not
            // occur properly since failover would not be able to traverse the hierarchy and update
            // the delegates properly
            removeSelf(invocation);

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
    * Closing the object
    */
   protected synchronized void closing() throws Throwable
   {
      state = CLOSING;
   }

   /**
    * Check the close has not already been done and
    * wait for all invocations to complete
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
    * Closed the object
    */
   protected synchronized void closed() throws Throwable
   {
      state = CLOSED;
      log.debug(this + " closed");
   }

   /**
    * Mark the object as no longer inuse
    */
   protected synchronized void done() throws Throwable
   {
      if (--inUseCount == 0)
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
            
      // We use a clone to avoid a deadlock where requests are made to close parent and child
      // concurrently
      
      Set clone;

      Set children = state.getChildren();
      
      if (children == null)
      {
         if (trace) { log.trace(this + " has no children"); }
         return;
      }
      
      synchronized (children)
      {
         clone = new HashSet(children);
      }
      
      // Cycle through the children this will do a depth first close
      for (Iterator i = clone.iterator(); i.hasNext();)
      {
         HierarchicalState child = (HierarchicalState)i.next();      
         Closeable del = (Closeable)child.getDelegate();
         try
         {
            del.closing();
            del.close();
         }
         catch (Throwable t)
         {
         	//We swallow exceptions in close/closing, this is because if the connection fails, it is naturally for code to then close
         	//in a finally block, it would not then be appropriate to throw an exception. This is a common technique
            if (trace)
            {
               log.trace("Failed to close", t);
            }
         }
      }
   }
   
   /**
    * Remove from parent
    * 
    * @param invocation the invocation
    */
   protected void removeSelf(Invocation invocation)
   {                  
      HierarchicalState state = ((DelegateSupport)invocation.getTargetObject()).getState();
            
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

