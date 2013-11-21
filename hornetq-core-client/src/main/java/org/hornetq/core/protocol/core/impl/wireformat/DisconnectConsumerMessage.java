package org.hornetq.core.protocol.core.impl.wireformat;


import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;

public class DisconnectConsumerMessage extends PacketImpl
{
   private long consumerId;

   public DisconnectConsumerMessage(final long consumerId)
   {
      super(DISCONNECT_CONSUMER);
      this.consumerId = consumerId;
   }

   public DisconnectConsumerMessage()
   {
      super(DISCONNECT_CONSUMER);
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerId);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerId = buffer.readLong();
   }

   public long getConsumerId()
   {
      return consumerId;
   }
}
