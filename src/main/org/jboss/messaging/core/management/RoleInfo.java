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
import static javax.management.openmbean.SimpleType.STRING;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class RoleInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;
   private static final String ROLE_TYPE_NAME = "RoleInfo";
   private static final String ROLE_TABULAR_TYPE_NAME = "RoleTabularInfo";
   private static final TabularType TABULAR_TYPE;
   private static final String[] ITEM_NAMES = new String[] { "name", "create",
         "read", "write" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "Name of the role", "Can the role create?", "Can the role read?",
         "Can the role write?" };
   private static final OpenType[] ITEM_TYPES = new OpenType[] { STRING,
         BOOLEAN, BOOLEAN, BOOLEAN };

   static
   {
      try
      {
         TYPE = new CompositeType(ROLE_TYPE_NAME, "Information for a Role",
               ITEM_NAMES, ITEM_DESCRIPTIONS, ITEM_TYPES);
         TABULAR_TYPE = new TabularType(ROLE_TABULAR_TYPE_NAME,
               "Table of RoleInfo", TYPE, new String[] { "name" });
      } catch (OpenDataException e)
      {
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String name;
   private final boolean create;
   private final boolean read;
   private final boolean write;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(RoleInfo[] infos)
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (RoleInfo roleInfo : infos)
      {
         data.put(roleInfo.toCompositeData());
      }
      return data;
   }

   // Constructors --------------------------------------------------

   public RoleInfo(String name, boolean create, boolean read, boolean write)
   {
      this.name = name;
      this.create = create;
      this.read = read;
      this.write = write;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public boolean isCreate()
   {
      return create;
   }

   public boolean isRead()
   {
      return read;
   }

   public boolean isWrite()
   {
      return write;
   }

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { name,
               create, read, write });
      } catch (OpenDataException e)
      {
         return null;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
