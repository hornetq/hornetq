/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.server.impl.fakes;

import java.util.LinkedList;
import java.util.List;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;

/**
 * 
 * A FakeConsumer
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeConsumer implements Consumer
{
   private HandleStatus statusToReturn = HandleStatus.HANDLED;
   
   private HandleStatus newStatus;
   
   private int delayCountdown = 0;
   
   private LinkedList<MessageReference> references = new LinkedList<MessageReference>();
   
   private Filter filter;
   
   public FakeConsumer()
   {         
   }
   
   public FakeConsumer(Filter filter)
   {
      this.filter = filter;
   }
   
   public synchronized MessageReference waitForNextReference(long timeout)
   {
      while (references.isEmpty() && timeout > 0)
      {
         long start = System.currentTimeMillis(); 
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {                 
         }
         timeout -= (System.currentTimeMillis() - start);
      }
      
      if (timeout <= 0)
      {
         throw new IllegalStateException("Timed out waiting for reference");
      }
      
      return references.removeFirst();
   }
   
   public synchronized void setStatusImmediate(HandleStatus newStatus)
   {
      this.statusToReturn = newStatus;
   }
   
   public synchronized void setStatusDelayed(HandleStatus newStatus, int numReferences)
   {
      this.newStatus = newStatus;
      
      this.delayCountdown = numReferences;
   }
   
   public synchronized List<MessageReference> getReferences()
   {
      return references;
   }
   
   public synchronized void clearReferences()
   {
      this.references.clear();
   }
   
   public synchronized HandleStatus handle(MessageReference reference)
   {
      if (filter != null)
      {
         if (filter.match(reference.getMessage()))
         {
            references.addLast(reference);
            reference.getQueue().referenceHandled();
            notify();
            
            return HandleStatus.HANDLED;
         }
         else
         {
            return HandleStatus.NO_MATCH;
         }
      }
      
      if (newStatus != null)
      {           
         if (delayCountdown == 0)
         {
            statusToReturn = newStatus;
            
            newStatus = null;
         }
         else
         {            
            delayCountdown--;
         }
      }
      
      if (statusToReturn == HandleStatus.HANDLED)
      {
         reference.getQueue().referenceHandled();
         references.addLast(reference);
         notify();
      }
      
      return statusToReturn;
   }
}
