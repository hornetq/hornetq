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

package org.jboss.messaging.tests.timing.core.remoting.network;

import org.apache.mina.common.IoFilterAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteRequest;

final class NetworkFailureFilter extends IoFilterAdapter
{
   Exception messageSentThrowsException = null;
   boolean messageSentDropsPacket = false;
   boolean messageReceivedDropsPacket = false;

   @Override
   public void messageSent(NextFilter nextFilter, IoSession session,
         WriteRequest writeRequest) throws Exception
   {
      if (messageSentThrowsException != null)
      {
         throw messageSentThrowsException;
      } else if (messageSentDropsPacket)
      {
         // do nothing
      } else
      {
         nextFilter.messageSent(session, writeRequest);
      }
   }

   @Override
   public void messageReceived(NextFilter nextFilter, IoSession session,
         Object message) throws Exception
   {
      if (messageReceivedDropsPacket)
      {
         // do nothing
      } else
      {
         super.messageReceived(nextFilter, session, message);
      }
   }
}