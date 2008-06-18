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

package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.Assert.fail;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.tests.unit.core.remoting.TestPacketHandler;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class Handler extends TestPacketHandler
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(Handler.class);
   

   // Attributes ----------------------------------------------------

   private int sleepTime;
   private TimeUnit timeUnit;
   private PacketReturner lastSender;
 
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public Handler(final long id)
   {
   	super(id);
   }

   // Public --------------------------------------------------------

   public void setSleepTime(int sleepTime, TimeUnit timeUnit)
   {
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
   }
   
   public PacketReturner getLastSender()
   {
      return lastSender;
   }
   
   // TestPacketHandler overrides -----------------------------------
   
   protected boolean isRequest(final Packet packet)
   {
      return packet.getTargetID() != PacketImpl.NO_ID_SET && packet.getResponseTargetID() != PacketImpl.NO_ID_SET;
   }
   
   @Override
   protected void doHandle(Packet packet, PacketReturner sender)
   {
      Assert.assertTrue(packet instanceof ConnectionCreateSessionResponseMessage);

      lastSender = sender;

      if (sleepTime > 0)
      {
         try
         {
            Thread.sleep(MILLISECONDS.convert(sleepTime, timeUnit));
         } catch (InterruptedException e)
         {
            fail();
         }
      }
                  
      ConnectionCreateSessionResponseMessage message = (ConnectionCreateSessionResponseMessage) packet;
      if (isRequest(message))
      {
         Pong response = new Pong(message.getSessionID(), true);
         response.normalize(message);
         try
         {
            sender.send(response);
         }
         catch (Exception e)
         {
            log.error("Failed to handle", e);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
