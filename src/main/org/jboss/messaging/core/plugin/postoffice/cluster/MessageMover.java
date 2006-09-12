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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.Binding;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;

/**
 * A MessageMover
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class MessageMover
{
   private TransactionRepository tr;
   
   private String nodeId;
   
   private RedistributionPolicy redistributionPolicy;
   
   public void moveMessages(Binding from, Binding to, int num) throws Throwable
   {
      if (!from.getNodeId().equals(this.nodeId))
      {
         throw new IllegalArgumentException("From binding must be on local node!");
      }
      
      if (to.getNodeId().equals(this.nodeId))
      {
         throw new IllegalArgumentException("To binding cannot be on local node");
      }
      
      //Consume the messages in a transaction - don't commit
      Queue fromQueue = from.getQueue();
      
      Transaction tx = tr.createTransaction();
      
      List dels = ((MeasuredQueue)fromQueue).getDeliveries(num);
      
      Iterator iter = dels.iterator();
      
      while (iter.hasNext())
      {
         Delivery del = (Delivery)iter.next();
         
         del.acknowledge(tx);         
      }
      
      
 
   } 
   
   public void calculateMovements(Collection nameLists)
   {
      Iterator iter = nameLists.iterator();
      
      while (iter.hasNext())
      {
         List bindings = (List)iter.next();
         
         if (bindings.size() > 1)
         {
            RedistributionOrder order = redistributionPolicy.calculate(bindings);
            
            if (order != null)
            {
               
            }
         }
      }
   }
}
