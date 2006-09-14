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

/**
 * A BasicRedistributonPolicy
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BasicRedistributionPolicy implements RedistributionPolicy
{
   private String localNodeId;
   
   private int MAX_MESSAGES_TO_MOVE = 100;
   
   public BasicRedistributionPolicy(String localNodeId)
   {
      this.localNodeId = localNodeId;
   }

   public RedistributionOrder calculate(List bindings)
   {
      Iterator iter = bindings.iterator();
      
      ClusteredBinding localBinding = null;
      
      while (iter.hasNext())
      {
         ClusteredBinding binding = (ClusteredBinding)iter.next();
         
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
      
      if (localBinding.getConsumptionRate() == 0 && localBinding.getMessageCount() > 0)
      {
         //No consumers on the queue - the messages are stranded
         //We should consider moving them somewhere else
         
         //We move messages to the node with the highest consumption rate
         
         iter = bindings.iterator();
         
         double maxRate = 0;
         
         ClusteredBinding maxRateBinding = null;
         
         while (iter.hasNext())
         {
            ClusteredBinding binding = (ClusteredBinding)iter.next();
            
            if (!binding.getNodeId().equals(localNodeId))
            {
               if (binding.getConsumptionRate() > maxRate)
               {
                  maxRate = binding.getConsumptionRate();
                  
                  maxRateBinding = binding;
               }
            }
         }
         
         if (maxRate > 0)
         {
            //Move messages to this node
            
            //How many should we move?
            int numberToMove = Math.min(MAX_MESSAGES_TO_MOVE, localBinding.getMessageCount());     
            
            return new RedistributionOrder(numberToMove, localBinding.getQueueName(), maxRateBinding.getNodeId());
         }
      }
      
      return null;
   }
}
