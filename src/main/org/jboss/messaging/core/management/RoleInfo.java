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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
   private static final String[] ITEM_NAMES = new String[] { "name", "send",
         "consume", "createDurableQueue", "deleteDurableQueue", "createNonDurableQueue", "deleteNonDurableQueue", "manage" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "Name of the role", "Can the role send messages?", "Can the role consume messages?",
         "Can the role create a durable queue?",
         "Can the role delete a durable queue?",
         "Can the role create a non durable queue?",
         "Can the role create a non durable queue?",
         "Can the user send management messages"};
   private static final OpenType[] ITEM_TYPES = new OpenType[] { STRING,
         BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN };

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

   final private boolean send;

   final private boolean consume;

   final private boolean createDurableQueue;

   final private boolean deleteDurableQueue;

   final private boolean createNonDurableQueue;

   final private boolean deleteNonDurableQueue;

   final private boolean manage;


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

   public static RoleInfo[] from(TabularData roles)
   {
      Collection values = roles.values();
      List<RoleInfo> infos = new ArrayList<RoleInfo>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData) object;
         String name = (String) compositeData.get("name");
         boolean send = (Boolean) compositeData.get("send");
         boolean consume = (Boolean) compositeData.get("consume");
         boolean createDurableQueue = (Boolean) compositeData.get("createDurableQueue");
         boolean deleteDurableQueue = (Boolean) compositeData.get("deleteDurableQueue");
         boolean createNonDurableQueue = (Boolean) compositeData.get("createNonDurableQueue");
         boolean deleteNonDurableQueue = (Boolean) compositeData.get("deleteNonDurableQueue");
         boolean manage = (Boolean) compositeData.get("manage");
         infos.add(new RoleInfo(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage));
      }

      return (RoleInfo[]) infos.toArray(new RoleInfo[infos.size()]);
   }
   
   public static RoleInfo[] from(Object[] roles)
   {
      //Collection values = roles.values();
      List<RoleInfo> infos = new ArrayList<RoleInfo>();
      for (Object object : roles)
      {
         Map<String, Object> compositeData = (Map<String, Object>) object;
         String name = (String) compositeData.get("name");
         boolean send = (Boolean) compositeData.get("send");
         boolean consume = (Boolean) compositeData.get("consume");
         boolean createDurableQueue = (Boolean) compositeData.get("createDurableQueue");
         boolean deleteDurableQueue = (Boolean) compositeData.get("deleteDurableQueue");
         boolean createNonDurableQueue = (Boolean) compositeData.get("createNonDurableQueue");
         boolean deleteNonDurableQueue = (Boolean) compositeData.get("deleteNonDurableQueue");
         boolean manage = (Boolean) compositeData.get("manage");
         infos.add(new RoleInfo(name, send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage));
      }

      return (RoleInfo[]) infos.toArray(new RoleInfo[infos.size()]);
   }
   
   // Constructors --------------------------------------------------


   public RoleInfo(String name, boolean send, boolean consume, boolean createDurableQueue, boolean deleteDurableQueue, boolean createNonDurableQueue, boolean deleteNonDurableQueue, boolean manage)
   {
      this.name = name;
      this.send = send;
      this.consume = consume;
      this.createDurableQueue = createDurableQueue;
      this.deleteDurableQueue = deleteDurableQueue;
      this.createNonDurableQueue = createNonDurableQueue;
      this.deleteNonDurableQueue = deleteNonDurableQueue;
      this.manage = manage;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public boolean isSend()
   {
      return send;
   }

   public boolean isConsume()
   {
      return consume;
   }

   public boolean isCreateDurableQueue()
   {
      return createDurableQueue;
   }

   public boolean isDeleteDurableQueue()
   {
      return deleteDurableQueue;
   }

   public boolean isCreateNonDurableQueue()
   {
      return createNonDurableQueue;
   }

   public boolean isDeleteNonDurableQueue()
   {
      return deleteNonDurableQueue;
   }

   public boolean isManage()
   {
      return manage;
   }

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { name,
               send, consume, createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage });
      } catch (OpenDataException e)
      {
         return null;
      }
   }

   @Override
   public String toString()
   {
       return "RolInfoe {name=" + name + ";" +
             "read=" + send + ";" +
             "write=" + consume + ";" +
             "createDurableQueue=" + createDurableQueue + "}" +
            "deleteDurableQueue=" + deleteDurableQueue + "}" +
            "createNonDurableQueue=" + createNonDurableQueue + "}" +
            "deleteNonDurableQueue=" + deleteNonDurableQueue + "}" +
            "manage=" + manage + "}";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
