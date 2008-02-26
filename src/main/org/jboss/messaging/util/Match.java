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
    a Match is the holder for the match string and the object to hold against it.
 */
public class Match<T>
{

   public static String WORD_WILDCARD = "^";
   private static String WORD_WILDCARD_REPLACEMENT = "[^.]+";
   public static String WILDCARD = "*";
   private static String WILDCARD_REPLACEMENT = ".+";
   private static final String DOT = ".";
   private static final String DOT_REPLACEMENT = "\\.";
   
   private String match;
   private Pattern pattern;
   private T value;



   public Match(String match)
   {
      this.match = match;
      String actMatch = match;
      //replace any regex characters
      if(WILDCARD.equals(match))
      {
         actMatch = WILDCARD_REPLACEMENT;
      }
      else
      {
         actMatch = actMatch.replace(DOT, DOT_REPLACEMENT);
         actMatch = actMatch.replace(WILDCARD,  WILDCARD_REPLACEMENT);
         actMatch = actMatch.replace(WORD_WILDCARD, WORD_WILDCARD_REPLACEMENT);
      }
      pattern = Pattern.compile(actMatch);

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


   public T getValue()
   {
      return value;
   }

   public void setValue(T value)
   {
      this.value = value;
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Match that = (Match) o;

      return !(match != null ? !match.equals(that.match) : that.match != null);

   }

   public int hashCode()
   {
      return (match != null ? match.hashCode() : 0);
   }

   /**
    * utility method to verify consistency of match
    * @param match the match to validate
    * @throws IllegalArgumentException if a match isnt valid
    */
   public static void verify(String match) throws IllegalArgumentException
   {
      if(match == null)
      {
         throw new IllegalArgumentException("match can not be null");
      }
      if(match.contains("*") && match.indexOf("*") < match.length() - 1)
      {
         throw new IllegalArgumentException("* can only be at end of match");
      }
   }

}
