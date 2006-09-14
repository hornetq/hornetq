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


/**
 * A MessagesRequest
 * 
 * Used when sending multiple messages non reliably across the group
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class MessagesRequest extends ClusterRequest
{
   static final int TYPE = 4;
   
   private List messageHolders;
   
   MessagesRequest()
   {      
   }
   
   MessagesRequest(List messageHolders)
   {
      this.messageHolders = messageHolders;
   }
   
   public void execute(PostOfficeInternal office) throws Exception
   {
      Iterator iter = messageHolders.iterator();
      
      while (iter.hasNext())
      {
         MessageHolder holder = (MessageHolder)iter.next();
         
         office.routeFromCluster(holder.getMessage(), holder.getRoutingKey(), holder.getQueueNameToNodeIdMap());
      }
   }
   
   public byte getType()
   {
      return TYPE;
   }

   public void read(DataInputStream in) throws Exception
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

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(messageHolders.size());
      Iterator iter = messageHolders.iterator();
      while (iter.hasNext())
      {
         MessageHolder holder = (MessageHolder)iter.next();
         holder.write(out);
      }
   }   
}

