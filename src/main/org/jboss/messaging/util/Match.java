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
package org.jboss.messaging.util;

import java.util.regex.Pattern;

/**
    * a Match is the holder for the match string and the object to hold against it.
 */
public class Match
{
   private String match;
   private Pattern pattern;
   private Object value;


   public Match(String match)
   {
      this.match = match;
      //compile in advance for performance reasons
      //check for dangling characters
      if(match.length() > 1)
         pattern = Pattern.compile(match);
      else
      {
         pattern = Pattern.compile(new StringBuilder("[").append(match).append("]").toString());
      }
   }


   public String getMatch()
   {
      return match;
   }

   public void setMatch(String match)
   {
      this.match = match;
   }

   public Pattern getPattern()
   {
      return pattern;
   }


   public Object getValue()
   {
      return value;
   }

   public void setValue(Object value)
   {
      this.value = value;
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Match that = (Match) o;

      if (match != null ? !match.equals(that.match) : that.match != null) return false;

      return true;
   }

   public int hashCode()
   {
      return (match != null ? match.hashCode() : 0);
   }
}
