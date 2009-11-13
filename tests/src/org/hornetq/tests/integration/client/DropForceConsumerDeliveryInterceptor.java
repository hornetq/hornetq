package org.hornetq.tests.integration.client;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;

public class DropForceConsumerDeliveryInterceptor implements Interceptor
{
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.SESS_FORCE_CONSUMER_DELIVERY)
      {
         return false;
      } else
      {
         return true;
      }
   }
}