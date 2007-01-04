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

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.Condition;

/**
 * A JMSCondition
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class JMSCondition implements Condition
{
   // Constants ------------------------------------------------------------------------------------

   private static final String QUEUE_PREFIX = "queue.";
   private static final String TOPIC_PREFIX = "topic.";

   // Static ---------------------------------------------------------------------------------------


   // Attributes -----------------------------------------------------------------------------------

   private boolean queue;
   private String name;

   // cache the hash code
   private int hash = -1;

   // Constructors ---------------------------------------------------------------------------------

   public JMSCondition(boolean queue, String name)
   {
      this.queue = queue;
      this.name = name;
   }

   public JMSCondition(String text)
   {
      if (text.startsWith(QUEUE_PREFIX))
      {
         queue = true;
         name = text.substring(QUEUE_PREFIX.length());
      }
      else if (text.startsWith(TOPIC_PREFIX))
      {
         queue = false;
         name = text.substring(TOPIC_PREFIX.length());
      }
      else
      {
         throw new IllegalArgumentException("Illegal text: " + text);
      }
   }

   // Condition implementation ---------------------------------------------------------------------

   public boolean matches(Condition routingCondition, MessageReference ref)
   {
      return equals(routingCondition);
   }

   public String toText()
   {
      return (queue ? QUEUE_PREFIX : TOPIC_PREFIX) + name;
   }

   // Public ---------------------------------------------------------------------------------------

   public boolean isQueue()
   {
      return queue;
   }

   public String getName()
   {
      return name;
   }

   public boolean equals(Object other)
   {
      if (!(other instanceof JMSCondition))
      {
         return false;
      }

      JMSCondition jmsCond = (JMSCondition)other;

      return ((jmsCond.queue == this.queue) && (jmsCond.name.equals(this.name)));
   }

   public int hashCode()
   {
      if (hash == -1)
      {
         hash = 17;
         hash = 37 * hash + (queue ? 0 : 1);
         hash = 37 * hash + name.hashCode();
      }

      return hash;
   }

   public String toString()
   {
      return toText();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
