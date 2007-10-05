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
package org.jboss.jms.client;

import java.util.HashSet;
import java.util.Set;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class FailoverValve2
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverValve2.class);


   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   // Only use this in trace mode
   private Set threads;

   private int count;
   
   private boolean locked;

   // Constructors ---------------------------------------------------------------------------------

   public FailoverValve2()
   {
      trace = log.isTraceEnabled();

      if (trace)
      {
         threads = new HashSet();
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public synchronized void enter()
   {
      if (trace) { log.trace(this + " entering"); }

      while (locked)
      {
         try
         {
            wait();
         }
         catch (InterruptedException ignore)
         {
         }
      }
      count++;

      if (trace)
      {
         threads.add(Thread.currentThread());
         log.trace(this + " entered");
      }
   }

   public synchronized void leave()
   {
      if (trace) { log.trace(this + " leaving"); }

      count--;

      if (trace) { threads.remove(Thread.currentThread()); }

      notifyAll();

      if (trace) { log.trace(this + " left"); }
   }

   public synchronized void close()
   {
      if (trace) { log.trace(this + " close " + (locked ? "LOCKED" : "UNLOCKED") + " valve"); }

      if (trace && threads.contains(Thread.currentThread()))
      {
         // Sanity check
         throw new IllegalStateException("Cannot close valve from inside valve");
      }

      // If the valve is already closed then any more invocations of close must block until the
      // valve is opened.

      while (locked)
      {
         if (trace) { log.trace(this + " is already closed, blocking until its opened"); }

         try
         {
            wait();
         }
         catch (InterruptedException ignore)
         {
         }

         if (!locked)
         {
            //If it was locked when we tried to close but is not now locked - then return immediately
            return;
         }
      }


      locked = true;

      while (count > 0)
      {
         try
         {
            wait();
         }
         catch (InterruptedException ignore)
         {
         }
      }

      if (trace) { log.trace(this + " closed"); }
   }

   public synchronized void open()
   {
      if (trace) { log.trace(this + " opening " + (locked ? "LOCKED" : "UNLOCKED") + " valve"); }

      if (!locked)
      {
         return;
      }

      locked = false;

      notifyAll();
   }

   public String toString()
   {
      return "FailoverValve[" + System.identityHashCode(this) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

