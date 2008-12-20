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

package org.jboss.messaging.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;

/**
 * A BindingsImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 11 Dec 2008 08:34:33
 *
 *
 */
public class BindingsImpl implements Bindings
{   
   private static final Logger log = Logger.getLogger(BindingsImpl.class);

   private final List<Binding> bindings = new CopyOnWriteArrayList<Binding>();

   private final AtomicInteger numberExclusive = new AtomicInteger(0);

   private final AtomicInteger pos = new AtomicInteger(0);
   
   private AtomicInteger weightCount = new AtomicInteger(0);
   
   private volatile Binding binding;
   
   public void addBinding(final Binding binding)
   {
      bindings.add(binding);

      if (binding.isExclusive())
      {
         numberExclusive.incrementAndGet();
      }
   }

   public List<Binding> getBindings()
   {
      return new ArrayList<Binding>(bindings);
   }

   public void removeBinding(final Binding binding)
   {
      bindings.remove(binding);

      if (binding.isExclusive())
      {
         numberExclusive.decrementAndGet();
      }
   }

   private Binding getNext(final ServerMessage message)
   {
      //It's not an exact round robin under concurrent access but that doesn't matter
      
      int startPos = -1;
      
      Binding ret = binding;
      
      while (true)
      {                          
         try
         {                     
            int thePos = pos.get();
            
            if (binding == null)
            {
               binding = bindings.get(thePos);
               
               ret = binding;
               
               weightCount.set(binding.getWeight());
            }
            
            Filter filter = binding.getQueue().getFilter();
            
            if ((filter != null && !filter.match(message)) || weightCount.get() == 0)
            {
               if (thePos == startPos)
               {
                  //Tried them all
                  return null;
               }
               
               if (startPos == -1)
               {                  
                  startPos = thePos;
               }
               
               advance();  
               
               continue;
            }
            else if (weightCount.decrementAndGet() <= 0)
            {
               advance();
            }
                     
            break;            
         }
         catch (IndexOutOfBoundsException e)
         {
            //Under concurrent access you might get IndexOutOfBoundsException so need to deal with this
            
            if (bindings.isEmpty())
            {
               return null;
            }
            else
            {
               pos.set(0);
               
               startPos = -1;
            }
         }                  
      }
      return ret;
   }
   
   private void advance()
   {
      if (pos.incrementAndGet() >= bindings.size())
      {
         pos.set(0);
      }
            
      binding = null;
   }
   
   public List<MessageReference> route(final ServerMessage message)
   {
      if (numberExclusive.get() > 0)
      {
         // We need to round robin
         
         List<MessageReference> refs = new ArrayList<MessageReference>(1);

         Binding binding = getNext(message);
            
         if (binding != null)
         {        
            Queue queue = binding.getQueue();
            
            MessageReference reference = message.createReference(queue);
   
            refs.add(reference);             
         }
         
         return refs;
      }
      else
      {
         // They all get the message
         
         // TODO - this can be optimised to avoid a copy
         
         if (!bindings.isEmpty())
         {   
            List<MessageReference> refs = new ArrayList<MessageReference>();
   
            for (Binding binding : bindings)
            {
               Queue queue = binding.getQueue();
   
               Filter filter = queue.getFilter();
   
               //Note we ignore any exclusive - this structure is concurrent so one could have been added
               //since the initial check on number of exclusive
               if (!binding.isExclusive() && (filter == null || filter.match(message)))
               {
                  MessageReference reference = message.createReference(queue);
   
                  refs.add(reference);
               }
            }
            
            return refs;
         }
         else
         {
            return Collections.<MessageReference>emptyList();
         }
      }
   }
}
