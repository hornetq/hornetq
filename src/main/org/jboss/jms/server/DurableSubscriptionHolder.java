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
package org.jboss.jms.server;

import java.io.Serializable;

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Topic;

/**
 * 
 * Holds state for a DurableSubscription
 *
 * @author <a href="tim.l.fox@gmail.com">Tim Fox/a>
 * @version $Revision$
 *
 * $Id$
 */
public class DurableSubscriptionHolder implements Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 6572385758103922254L;
   
   // Attributes ----------------------------------------------------
   
   private String name;
   private Queue queue;
   private Topic topic;
   private String selector;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public DurableSubscriptionHolder(String name, Topic topic, Queue queue, String selector)
   {
      this.name = name;
      this.queue = queue;
      this.topic = topic;
      this.selector = selector;
   }

   // Public --------------------------------------------------------
   

   public Queue getQueue()
   {
      return queue;
   }
   
   public Topic getTopic()
   {
      return topic;
   }
   
   public String getName()
   {
      return name;
   }
   
   public String getSelector()
   {
      return selector;
   }
   

   // Interface XXX implementation ----------------------------------
   
   // Object overrides -------------------------------------------
   
   public boolean equals(Object other)
   {
      if (this == other) return true;
      
      if (!(other instanceof DurableSubscriptionHolder)) return false;
      
      DurableSubscriptionHolder dOther = (DurableSubscriptionHolder)other;
      
      if (dOther.name == null || this.name == null) return false;
      
      return dOther.name.equals(this.name);
   }
   
   public int hashCode()
   {
      return name.hashCode();
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
     
}
