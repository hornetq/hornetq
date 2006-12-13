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

 * @see org.jboss.test.messaging.jms.VeryBasicValveTest
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *  */
public class Valve
{
   boolean opened;
   boolean closed;


   public boolean open() throws Exception
   {
      return open(true);
   }

   public synchronized boolean open(boolean wait) throws Exception
   {
      // already opened? then needs to wait to be closed
      if (opened)
      {
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
         opened = true;
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
         throw new IllegalStateException("Valve is already closed");
      }
      closed = true;
      notifyAll();
   }
}
