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
package org.jboss.messaging.core.impl;

import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Queue;

/**
 * 
 * A BindingImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BindingImpl implements Binding
{
   private int nodeID;
   
   private Condition condition;
   
   private Queue queue;
   
   private boolean allNodes;
   
   private boolean hashAssigned;
   
   private int hash;
      
   public BindingImpl(int nodeID, Condition condition, Queue queue, boolean allNodes)
   {
      this.nodeID = nodeID;
      
      this.condition = condition;
      
      this.queue = queue;
      
      this.allNodes = allNodes;
   }
   
   public Condition getCondition()
   {
      return condition;
   }

   public int getNodeID()
   {
      return nodeID;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public boolean isAllNodes()
   {
      return allNodes;
   }
     
   public boolean equals(Object other)
   {
      if (this == other)
      {
         return true;
      }
      Binding bother = (Binding)other;
      
      return (this.nodeID == bother.getNodeID()) && (this.allNodes == bother.isAllNodes()) &&
              this.condition.equals(bother.getCondition()) &&
              this.queue.equals(bother.getQueue());
   }
   
   public int hashCode()
   {
      if (!hashAssigned)
      {
         hash = 17;
         hash = 37 * hash + nodeID;
         hash = 37 * hash + condition.hashCode();
         hash = 37 * hash + queue.hashCode();
         hash = 37 * hash + (allNodes ? 0 : 1);
         
         hashAssigned = true;
      }

      return hash;
   }
}
