/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.ResponseHandler;

/**
 *
 * A ResponseHandlerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ResponseHandlerImpl implements ResponseHandler
{
   private final long id;

   private Packet response;

   private boolean failed = false;

   public ResponseHandlerImpl(final long id)
   {
      this.id = id;
   }

   public long getID()
   {
      return id;
   }

   public synchronized void handle(final Object remotingConnectionID, final Packet packet)
   {
      if (failed)
      {
         //Ignore any responses that come back after we timed out
         return;
      }

      response = packet;

      notify();
   }

   public void setFailed()
   {
      failed = true;
   }

   public synchronized Packet waitForResponse(final long timeout)
   {
      if (failed)
      {
         throw new IllegalStateException("Cannot wait for response - handler has failed");
      }

      long toWait = timeout;
      long start = System.currentTimeMillis();

      while (response == null && toWait > 0)
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


      if (response == null)
      {
         failed = true;
      }

      return response;
   }


   public void reset()
   {
      response = null;
   }

}