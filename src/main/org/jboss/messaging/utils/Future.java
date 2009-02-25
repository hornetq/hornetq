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
package org.jboss.messaging.utils;


/**
 * A Future
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Future implements Runnable
{
   private boolean done;
   
   public synchronized boolean await(final long timeout)
   {
      long toWait = timeout;
      
      long start = System.currentTimeMillis();

      while (!done && toWait > 0)
      {
         try
         {
            wait(toWait);
         }
         catch (InterruptedException e)
         {
         }

         long now = System.currentTimeMillis();

         toWait -= now - start;

         start = now;
      }
      
      return done;
   }
   
   public synchronized void run()
   {
      done = true;
      
      notify();
   }
   
}
