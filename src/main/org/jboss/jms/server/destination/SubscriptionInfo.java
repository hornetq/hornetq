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
package org.jboss.jms.server.destination;

import java.io.Serializable;

/**
 * A SubscriptionInfo
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class SubscriptionInfo implements Serializable
{
   private static final long serialVersionUID = -38689006079435295L;

   private String id;
   
   private boolean durable;
   
   private String name;
   
   private String clientID;
   
   private String selector;
   
   private int messageCount;
   
   private int maxSize;
   
   public SubscriptionInfo(String id, boolean durable, String name, String clientID, String selector, int messageCount, int maxSize)
   {
      this.id = id;
      this.durable = durable;
      this.name = name;
      this.clientID = clientID;
      this.selector = selector;
      this.messageCount = messageCount;
      this.maxSize = maxSize;
   }
   
   public String getId()
   {
      return id;
   }

   public String getClientID()
   {
      return clientID;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public int getMaxSize()
   {
      return maxSize;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public String getName()
   {
      return name;
   }

   public String getSelector()
   {
      return selector;
   }

}
