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

package org.jboss.jms.util;

import org.jboss.logging.Logger;


/**
 * This class is used to guarantee only one thread will be performing a given function, and if any other
 * thread tries to execute the same functionality it will just ignored
 * <pre>
     Valve valve = new Valve();
        if (valve.open())
        {
             try
             {
                doSomething();
             }
             finally
             {
                valve.close();
             }
        }
        else
        {
             System.out.println("Nothing to be done
        }
  </pre>

   <p>Notice that you can call close only once, so only call close if you were able to open the valve.</p>
   <p>After its usage if you decide to reset you should just create a new Valve in a safe synchronized block, so
        if any other thread still using the variable you do it in a safe way </p>

 * @see org.jboss.test.messaging.util.VeryBasicValveTest
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *  */
public class Valve
{
   private static final Logger log = Logger.getLogger(Valve.class);
   private boolean trace = log.isTraceEnabled();

   boolean opened;
   boolean closed;

   Thread threadOwner;

   int refereceCountOpen=0;


   public synchronized boolean isOpened()
   {
      return opened;
   }

   /** If the Valve is opened, will wait until the valve is closed */
   public synchronized boolean isOpened(boolean wait) throws Exception
   {
      if (wait && opened)
      {
         if (!closed && threadOwner != Thread.currentThread())
         {
            if (trace) log.trace("threadOwner= " + threadOwner + " and currentThread=" + Thread.currentThread());
            if (trace) log.trace("Waiting valve to be closed");
            this.wait();
            if (trace) log.trace("Valve was closed");
         }
         else
         {
            if (trace) log.trace("This is ThreadOwner, so Valve won't wait");
         }
         return opened;
      }
      else
      {
         return false;
      }

   }

   public boolean open() throws Exception
   {
      return open(true);
   }

   public synchronized boolean open(boolean wait) throws Exception
   {
      if (threadOwner==Thread.currentThread())
      {
         if (trace) log.trace("Valve was opened again by thread owner");
         refereceCountOpen++;
         return true;
      }
      // already opened? then needs to wait to be closed
      if (opened)
      {
         if (trace) log.trace("Valve being opened and time.wait");
         // if not closed yet, will wait to be closed
         if (!closed)
         {
            if (wait)
            {
               this.wait();
            }
         }
         return false;
      } else
      {
         if (trace) log.trace("Valve being opened and this thread is the owner for this lock");
         refereceCountOpen++;
         opened = true;
         threadOwner = Thread.currentThread();
         return true;
      }
   }

   public synchronized void close()
   {
      if (!opened)
      {
         throw new IllegalStateException("Open must be called first");
      }
      if (closed)
      {
         log.warn("Valve was already closed", new Exception());
      }
      refereceCountOpen--;
      if (refereceCountOpen==0)
      {
         if (trace) log.trace("Closing Valve");
         closed = true;
         notifyAll();
      }
      else
      {
         if (trace) log.trace("Valve.close called but there referenceCountOpen=" + refereceCountOpen);
      }
   }
}
