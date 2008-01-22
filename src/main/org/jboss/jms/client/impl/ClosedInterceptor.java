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
package org.jboss.jms.client.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.jms.IllegalStateException;

import org.jboss.messaging.util.Logger;


/**
 * An interceptor for checking closed state. It waits for other invocations to complete before
 * allowing the close. I.e. it performs the function of a "valve".
 *
 * This interceptor is PER_INSTANCE.
 *
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com>Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class ClosedInterceptor implements InvocationHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClosedInterceptor.class);
   private static boolean trace = log.isTraceEnabled();

   private static final int NOT_CLOSED = 0;
   private static final int IN_CLOSING = 1;
   private static final int CLOSING = 2;
   private static final int IN_CLOSE = 3; // performing the close
   private static final int CLOSED = -1;

   // Attributes ----------------------------------------------------


   // The current state of the object guarded by this interceptor
   private int state = NOT_CLOSED;

   // The inuse count
   private int inUseCount;


   private Object target;

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

   public ClosedInterceptor(Object target)
   {
      state = NOT_CLOSED;
      inUseCount = 0;
      this.target=target;
   }

   // Public --------------------------------------------------------


   public Object getTarget()
   {
      return target;
   }

   public String toString()
   {
      return "ClosedInterceptor for (" + target + ")";
   }

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ClosedInterceptor";
   }

   public Object invoke(Object o, Method method, Object[] args) throws Throwable
   {
      String methodName = method.getName();

      boolean isClosing = methodName.equals("closing");
      boolean isClose = methodName.equals("close");

      synchronized(this)
      {
         // object "in use", increment inUseCount
         if (state == CLOSED)
         {
            if (isClosing || isClose)
            {
               return new Long(-1);
            }
            log.error(this + ": method " + methodName + "() did not go through, " +
                             "the interceptor is " + stateToString(state));

            throw new IllegalStateException("The object is closed");
         }
         ++inUseCount;
      }

      try
      {
         return method.invoke(target, args);
      }
      catch (InvocationTargetException exT)
      {
         Throwable t = exT.getCause();

         if (isClosing || isClose)
      	{
            log.warn(t.getMessage(), t);
	      	//We swallow exceptions in close/closing, this is because if the connection fails, it is naturally for code to then close
	      	//in a finally block, it would not then be appropriate to throw an exception. This is a common technique
	      	//Close should ALWAYS (well apart from Errors) succeed irrespective of whether the actual connection to the server is alive.
	      	return new Long(-1);
      	}
      	throw t;
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
      log.trace(this + " closed");
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

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

