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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.util.StreamUtils;

/**
 * A PullMessagesRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class PullMessagesRequest extends TransactionRequest implements ClusterTransaction
{
   private static final Logger log = Logger.getLogger(PullMessagesRequest.class);
      
   private String queueName;
   
   private int numMessages;
   
   private List reliableDels;
   
   static final int TYPE = 5;
         
   PullMessagesRequest()
   {      
   }
        
   PullMessagesRequest(String nodeId, long txId, long checkChannelID, String queueName, int numMessages)
   {
      super(nodeId, txId, true, checkChannelID);
      
      this.queueName = queueName;
      
      this.numMessages = numMessages;
   }
   
   PullMessagesRequest(String nodeId, long txId)
   {
      super(nodeId, txId, false);
   }

   Object execute(PostOfficeInternal office) throws Throwable
   { 
      TransactionId id = new TransactionId(nodeId, txId);
      
      if (hold)
      {         
         List dels = office.getDeliveries(queueName, numMessages);
         
         PullMessagesResponse response = new PullMessagesResponse(dels.size());
         
         if (!dels.isEmpty())
         {
            Iterator iter = dels.iterator();
            
            Delivery del = (Delivery)iter.next();
            
            if (del.getReference().isReliable())
            {
               //Add it to internal list
               if (reliableDels == null)
               {
                  reliableDels  = new ArrayList();
                  
                  reliableDels.add(del);
               }
            }
            else
            {
               //We can ack it now
               del.acknowledge(null);
            }
            
            response.addMessage(del.getReference().getMessage());
         }
         
         if (reliableDels != null)
         {
            //Add this to the holding area
            office.holdTransaction(id, this);
         }
          
         //Convert to bytes since the response isn't serializable (nor do we want it to be)
         byte[] bytes = StreamUtils.toBytes(response);
         
         return bytes;
      }
      else
      {
         office.commitTransaction(id);
         
         return null;
      }
   }

   byte getType()
   {
      return TYPE;
   }

   public boolean check(PostOfficeInternal office) throws Exception
   {
      // If the messages DON'T exist in the database then we should commit the transaction
      // Since the acks have already been processed persistently
      
      // otherwise we should roll it back
      
      Iterator iter = reliableDels.iterator();
      
      //We only need to check one of them since they would all have been acked in a tx      
      
      Delivery del = (Delivery)iter.next();
      
      //We store the channelID of one of the channels that the message was persisted in
      //it doesn't matter which one since they were all inserted in the same tx
      
      if (office.referenceExistsInStorage(checkChannelID, del.getReference().getMessageID()))
      {
         //We should rollback
         return false;
      }
      else
      {
         //We should commit
         return true;
      }
   }

   public void commit(PostOfficeInternal office) throws Throwable
   {
      //We need to ack the deliveries
      
      Iterator iter = reliableDels.iterator();
      
      while (iter.hasNext())
      {
         Delivery del = (Delivery)iter.next();
         
         //We need to ack them in memory only
         //since they would have been acked on the pulling node
         LocalClusteredQueue queue = (LocalClusteredQueue)del.getObserver();
         
         queue.acknowledgeFromCluster(del);
      }
   }

   public void rollback(PostOfficeInternal office) throws Throwable
   {
      //We need to cancel the deliveries
      
      Iterator iter = reliableDels.iterator();
      
      while (iter.hasNext())
      {
         Delivery del = (Delivery)iter.next();
         
         del.cancel();
      }      
   }
   
   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
      
      if (hold)
      {
         queueName = in.readUTF();
         
         numMessages = in.readInt();
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      if (hold)
      {      
         out.writeUTF(queueName);
         
         out.writeInt(numMessages);
      }
   }


}
