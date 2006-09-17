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



/**
 * A TransactionRequest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
abstract class TransactionRequest extends ClusterRequest implements ClusterTransaction
{
   private String nodeId;
   
   private long txId;
 
   private boolean hold;
   
   TransactionRequest()
   {      
   }
      
   TransactionRequest(String nodeId, long txId, boolean hold)
   {
      this.nodeId = nodeId;
      
      this.txId= txId;

      this.hold = hold;
   }
   
   public void execute(PostOfficeInternal office) throws Exception
   { 
      TransactionId id = new TransactionId(nodeId, txId);
      
      if (hold)
      {
         office.holdTransaction(id, this);
      }
      else
      {
         office.commitTransaction(id);
      }
   }   
   
   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readUTF();
      
      txId = in.readLong();
      
      hold = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(nodeId);
      
      out.writeLong(txId);
      
      out.writeBoolean(hold);
   }  
}


