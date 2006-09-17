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

import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.plugin.postoffice.Binding;

/**
 * A BasicRedistributonPolicy
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultRedistributionPolicy implements RedistributionPolicy
{
   private String localNodeId;
   
   private int MAX_MESSAGES_TO_MOVE = 100;
   
   public DefaultRedistributionPolicy(String localNodeId)
   {
      this.localNodeId = localNodeId;
   }

   public RedistributionOrder calculate(List bindings)
   {
      Iterator iter = bindings.iterator();
      
      Binding localBinding = null;
      
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         
         if (binding.getNodeId().equals(localNodeId))
         {
            localBinding = binding;
            
            break;
         }
      }
      
      if (localBinding == null)
      {
         return null;
      }
      
      ClusteredQueue queue = (ClusteredQueue)localBinding.getQueue();
      
      if (queue.getGrowthRate() == 0 && queue.getMessageCount() > 0)
      {
         //No consumers on the queue - the messages are stranded
         //We should consider moving them somewhere else
         
         //We move messages to the node with the highest consumption rate
         
         iter = bindings.iterator();
         
         double maxRate = 0;
         
         Binding maxRateBinding = null;
         
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
            
            ClusteredQueue theQueue = (ClusteredQueue)binding.getQueue();
            
            if (!binding.getNodeId().equals(localNodeId))
            {
               double rate = theQueue.getGrowthRate();
               
               if (rate > maxRate)
               {
                  maxRate = rate;
                  
                  maxRateBinding = binding;
               }
            }
         }
         
         if (maxRate > 0)
         {
            //Move messages to this node
            
            //How many should we move?
            int numberToMove = Math.min(MAX_MESSAGES_TO_MOVE, queue.getMessageCount());     
            
            return new RedistributionOrder(numberToMove, queue, maxRateBinding.getNodeId());
         }
      }
      
      return null;
   }
}
