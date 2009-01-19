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

import static javax.management.openmbean.SimpleType.STRING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.jboss.messaging.util.Pair;

/**
 * Info for a Message property.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class PairsInfo
{
   // Constants -----------------------------------------------------

   public static final TabularType TABULAR_TYPE;

   private static CompositeType ROW_TYPE;

   static
   {
      try
      {
         ROW_TYPE = new CompositeType("Pair",
                                      "Pair",
                                      new String[] { "a", "b" },
                                      new String[] { "First item of the pair", "Second item of the pair" },
                                      new OpenType[] { STRING, STRING });
         TABULAR_TYPE = new TabularType("PairInfo", "Pair", ROW_TYPE, new String[] { "a" });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   public static List<Pair<String, String>> from(TabularData connectorInfos)
   {
      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();
      Collection values = connectorInfos.values();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         pairs.add(new Pair<String, String>((String)compositeData.get("a"), (String)compositeData.get("b")));
      }
      return pairs;
   }
   
   public static TabularData toTabularData(Pair<String, String> pair)
   {
      List<Pair<String, String>> list = new ArrayList<Pair<String, String>>();
      list.add(pair);
      PairsInfo info = new PairsInfo(list);
      return info.toTabularData();
   }

   public static TabularData toTabularData(List<Pair<String, String>> pairs)
   {
      PairsInfo info = new PairsInfo(pairs);
      return info.toTabularData();
   }

   // Attributes ----------------------------------------------------

   private final List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();

   // Private -------------------------------------------------------

   private PairsInfo(List<Pair<String, String>> pairs)
   {
      if (pairs != null)
      {
         for (Pair<String, String> pair : pairs)
         {
            this.pairs.add(pair);
         }
      }
   }

   private TabularData toTabularData()
   {
      try
      {
         TabularDataSupport data = new TabularDataSupport(TABULAR_TYPE);
         for (Pair<String, String> pair : pairs)
         {
            data.put(new CompositeDataSupport(ROW_TYPE, new String[] { "a", "b" }, new Object[] { pair.a, pair.b }));
         }
         return data;
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         return null;
      }
   }

}