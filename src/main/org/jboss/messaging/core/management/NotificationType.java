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


package org.jboss.messaging.core.management;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:fox@redhat.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public enum NotificationType
{
   BINDING_ADDED, BINDING_REMOVED, ADDRESS_ADDED, ADDRESS_REMOVED, CONSUMER_CREATED, CONSUMER_CLOSED;
   
   public static final int BINDING_ADDED_INDEX = 0;
   
   public static final int BINDING_REMOVED_INDEX = 1;
   
   public static final int ADDRESS_ADDED_INDEX = 2;
   
   public static final int ADDRESS_REMOVED_INDEX = 3;
   
   public static final int CONSUMER_CREATED_INDEX = 4;
   
   public static final int CONSUMER_CLOSED_INDEX = 5;
      
   public static NotificationType fromInt(final int index)
   {
      switch (index)
      {
         case BINDING_ADDED_INDEX:
         {
            return NotificationType.BINDING_ADDED;
         }
         case BINDING_REMOVED_INDEX:
         {
            return NotificationType.BINDING_REMOVED;
         }
         case ADDRESS_ADDED_INDEX:
         {
            return NotificationType.ADDRESS_ADDED;
         }
         case ADDRESS_REMOVED_INDEX:
         {
            return NotificationType.ADDRESS_REMOVED;
         }
         case CONSUMER_CREATED_INDEX:
         {
            return NotificationType.CONSUMER_CREATED;
         }
         case CONSUMER_CLOSED_INDEX:
         {
            return NotificationType.CONSUMER_CLOSED;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid index " + index);
         }
      }
   }
   
   public int toInt()
   {
      if (this.equals(NotificationType.BINDING_ADDED))
      {
         return BINDING_ADDED_INDEX;
      }
      else if (this.equals(NotificationType.BINDING_REMOVED))
      {
         return BINDING_REMOVED_INDEX;
      }
      else if (this.equals(NotificationType.ADDRESS_ADDED))
      {
         return ADDRESS_ADDED_INDEX;
      }
      else if (this.equals(NotificationType.ADDRESS_REMOVED))
      {
         return ADDRESS_REMOVED_INDEX;
      }
      else if (this.equals(NotificationType.CONSUMER_CREATED))
      {
         return CONSUMER_CREATED_INDEX;
      }
      else if (this.equals(NotificationType.CONSUMER_CLOSED))
      {
         return CONSUMER_CLOSED_INDEX;
      }
      else
      {
         throw new IllegalArgumentException("Cannot convert");
      }
   }
}