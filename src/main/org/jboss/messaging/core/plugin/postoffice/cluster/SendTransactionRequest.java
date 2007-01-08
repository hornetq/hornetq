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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.util.StreamUtils;

/**
 * A SendTransactionRequest
 * 
 * Used for sending persistent messages transactionally across the network
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
class SendTransactionRequest extends TransactionRequest
{
   // Constants -----------------------------------------------------

   static final int TYPE = 8;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private List messageHolders;

   // Constructors --------------------------------------------------

   SendTransactionRequest()
   {
   }

   SendTransactionRequest(int nodeId, long txId, List messageHolders, long channelID)
   {
      super(nodeId, txId, true, channelID);

      this.messageHolders = messageHolders;
   }

   SendTransactionRequest(int nodeId, long txId)
   {
      super(nodeId, txId, false);
   }

   // TransactionRequest overrides -----------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      int b = in.readByte();
      if (b == StreamUtils.NULL)
      {
         messageHolders = null;
      }
      else
      {
         int size = in.readInt();
         messageHolders = new ArrayList(size);
         for (int i = 0; i < size; i++)
         {
            MessageHolder holder = new MessageHolder();
            holder.read(in);
            messageHolders.add(holder);
         }
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      if (messageHolders != null)
      {
         out.writeByte(StreamUtils.LIST);
         out.writeInt(messageHolders.size());
         Iterator iter = messageHolders.iterator();
         while (iter.hasNext())
         {
            MessageHolder holder = (MessageHolder)iter.next();
            holder.write(out);
         }
      }
      else
      {
         out.writeByte(StreamUtils.NULL);
      }
   }

   // ClusterTransaction implementation -----------------------------

   public void commit(PostOfficeInternal office) throws Exception
   {
      Iterator iter = messageHolders.iterator();

      while (iter.hasNext())
      {
         MessageHolder holder = (MessageHolder)iter.next();

         office.routeFromCluster(holder.getMessage(), holder.getRoutingKey(), holder.getQueueNameToNodeIdMap());
      }
   }

   public void rollback(PostOfficeInternal office) throws Exception
   {
      //NOOP
   }

   public boolean check(PostOfficeInternal office) throws Exception
   {
      //If the messages exist in the database then we should commit the transaction
      //otherwise we should roll it back

      Iterator iter = messageHolders.iterator();

      //We only need to check that one of the refs made it to the database - the refs would have
      //been inserted into the db transactionally, so either they're all there or none are
      MessageHolder holder = (MessageHolder)iter.next();

      //We store the channelID of one of the channels that the message was persisted in
      //it doesn't matter which one since they were all inserted in the same tx

      if (office.referenceExistsInStorage(checkChannelID, holder.getMessage().getMessageID()))
      {
         //We can commit
         return true;
      }
      else
      {
         //We should rollback
         return false;
      }
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return TYPE;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("SendTransactionRequest[");
      if (messageHolders == null)
      {
         sb.append("EMPTY");
      }
      else
      {
         for(Iterator i = messageHolders.iterator(); i.hasNext(); )
         {
            sb.append(((MessageHolder)i.next()).getMessage());
            if (i.hasNext())
            {
               sb.append(',');
            }
         }
      }
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

