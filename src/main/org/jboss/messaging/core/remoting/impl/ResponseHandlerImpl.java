/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.ResponseHandler;

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

   public synchronized void handle(final Packet packet, final PacketReturner sender)
   {
      if (failed)
      {
         //Ignore any responses that come back after we timed out
         return;
      }

      this.response = packet;

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