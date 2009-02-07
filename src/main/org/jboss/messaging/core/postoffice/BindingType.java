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


package org.jboss.messaging.core.postoffice;

/**
 * A BindingType
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 22 Dec 2008 13:37:23
 *
 *
 */
public enum BindingType
{   
   LOCAL_QUEUE, REMOTE_QUEUE, DIVERT;
     
   public static final int LOCAL_QUEUE_INDEX = 0;
   
   public static final int REMOTE_QUEUE_INDEX = 1;
   
   public static final int DIVERT_INDEX = 2;
   
   public static BindingType fromOrdinal(final int index)
   {
      switch (index)
      {
         case LOCAL_QUEUE_INDEX:
         {
            return BindingType.LOCAL_QUEUE;
         }
         case REMOTE_QUEUE_INDEX:
         {
            return BindingType.REMOTE_QUEUE;
         }
         case DIVERT_INDEX:
         {
            return BindingType.DIVERT;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid index " + index);
         }
      }
   }
   
   public int toInt()
   {
      if (this.equals(BindingType.LOCAL_QUEUE))
      {
         return LOCAL_QUEUE_INDEX;
      }
      else if (this.equals(BindingType.REMOTE_QUEUE))
      {
         return REMOTE_QUEUE_INDEX;
      }
      else if (this.equals(BindingType.DIVERT))
      {
         return DIVERT_INDEX;
      }
      else
      {
         throw new IllegalArgumentException("Cannot convert");
      }
   }
   
  
}
