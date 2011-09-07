/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.PacketImpl;

public final class ReplicationCurrentPagesMessage extends PacketImpl
{

   private Map<SimpleString, Collection<Integer>> info;

   /**
    * @param type
    */
   public ReplicationCurrentPagesMessage()
   {
      super(REPLICATION_CURRENT_PAGES_INFO);
   }

   /**
    * @param info
    */
   public ReplicationCurrentPagesMessage(Map<SimpleString, Collection<Integer>> info)
   {
      this();
      this.info = info;
   }

   @Override
   public void decodeRest(HornetQBuffer buffer)
   {
      info = new HashMap<SimpleString, Collection<Integer>>();
      int entries = buffer.readInt();
      for (int i = 0; i < entries; i++)
      {
         SimpleString name = buffer.readSimpleString();
         int nPages = buffer.readInt();
         List<Integer> ids = new ArrayList<Integer>(nPages);
         for (int j = 0; j < nPages; j++)
         {
            ids.add(Integer.valueOf(buffer.readInt()));
         }
         info.put(name, ids);
      }
   }

   @Override
   public void encodeRest(HornetQBuffer buffer)
   {
      buffer.writeInt(info.size());
      for (Entry<SimpleString, Collection<Integer>> entry : info.entrySet())
      {
         buffer.writeSimpleString(entry.getKey());
         Collection<Integer> value = entry.getValue();
         buffer.writeInt(value.size());
         for (Integer id : value)
         {
            buffer.writeInt(id);
         }
      }
   }

   public Map<SimpleString, Collection<Integer>> getInfo()
   {
      return info;
   }
}
