/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.network;

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