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


package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.server.SendLock;


/**
 * A SendLockImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 27 Oct 2008 12:42:37
 *
 *
 */
public class SendLockImpl implements SendLock
{
   private boolean locked;
   
   private int count;
      
   public synchronized void lock()
   {
      while (count > 0 || locked)
      {
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {            
         }
      }  
      
      locked = true;      
   }
   
   public synchronized void unlock()
   {
      locked = false;
      
      notifyAll();
   }
     
   public synchronized void beforeSend()   
   {
      while (locked)
      {
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {            
         }
      }    
      
      count++;      
   }
   
   public synchronized void afterSend()
   {
      count--;
      
      if (count < 0)
      {
         throw new IllegalStateException("called afterSend() too many times");
      }
      
      if (count == 0)
      {         
         notifyAll();
      }
   }

}
