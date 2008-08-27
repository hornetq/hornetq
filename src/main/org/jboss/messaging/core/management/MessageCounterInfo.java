/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.STRING;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularType;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessageCounterInfo
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageCounterInfo.class);

   public static final CompositeType TYPE;
   private static final String MESSAGE_TYPE_NAME = "MessageCounterInfo";
   private static final String MESSAGE_TABULAR_TYPE_NAME = "TabularMessageCounterInfo";
   private static final String[] ITEM_NAMES = new String[] { "name",
         "subscription", "durable", "count", "countDelta", "depth",
         "depthDelta", "timestamp" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "Name of the Queue", "Name of the subscription",
         "Is the queue durable?", "Message count", "Message count delta",
         "Depth", "Depth delta", "Timestamp of the last increment" };
   private static final OpenType[] TYPES;
   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, STRING, BOOLEAN, INTEGER, INTEGER,
               INTEGER, INTEGER, STRING };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
               "Information for a MessageCounter", ITEM_NAMES,
               ITEM_DESCRIPTIONS, TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
               "Information for Tabular MessageCounter", TYPE,
               new String[] { "name" });
      } catch (OpenDataException e)
      {
         log.error("Unable to create open types for a MessageCounter", e);
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String name;
   private final String subscription;
   private final boolean durable;
   private final int count;
   private final int countDelta;
   private final int depth;
   private final int depthDelta;
   private final String timestamp;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessageCounterInfo(final String name, final String subscription,
         final boolean durable, final int count, final int countDelta,
         final int depth, final int depthDelta, final String timestamp)
   {
      this.name = name;
      this.subscription = subscription;
      this.durable = durable;
      this.count = count;
      this.countDelta = countDelta;
      this.depth = depth;
      this.depthDelta = depthDelta;
      this.timestamp = timestamp;
   }

   // Public --------------------------------------------------------

   public CompositeData toCompositeData()
   {
      try
      {

         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { name,
               subscription, durable, count, countDelta, depth, depthDelta,
               timestamp });
      } catch (OpenDataException e)
      {
         log.error("Unable to create a CompositeData from a MessageCounter", e);
         return null;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
