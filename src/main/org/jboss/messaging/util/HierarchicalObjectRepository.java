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

import java.util.HashMap;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HierarchicalObjectRepository<E> implements HierarchicalRepository<E>
{
   /**
    * The default Match to fall back to
    */
   E defaultmatch;

   /**
    * all the matches
    */
   HashMap<String, Match> matches = new HashMap<String, Match>();

   /**
    * Add a new match to the repository
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
   public void addMatch(String match, E value)
   {
      //create a match and add it to the list
      if(match.equals("*"))
      {
         defaultmatch = value;   
      }
      else
      {
         Match<E> match1 = new Match<E>(match);
         match1.setValue(value);
         matches.put(match, match1);
      }
   }

   /**
    * return the value held against the nearest match
    * @param match the match to look for
    * @return the value
    */
   public E getMatch(String match)
   {
      HashMap<String, Match<E>> possibleMatches = getPossibleMatches(match);
      E actualMatch  = getActualMatch(match, possibleMatches);
      return actualMatch != null ? actualMatch:defaultmatch;
   }

   /**
    * remove a match from the repository
    * @param match the match to remove
    */
   public void removeMatch(String match)
   {
      matches.remove(match);
   }

   /**
    * set the default value to fallback to if none found
    * @param defaultValue the value
    */
   public void setDefault(E defaultValue)
   {
      defaultmatch = defaultValue;
   }

   private HashMap<String, Match<E>> getPossibleMatches(String match)
   {
      HashMap<String, Match<E>> possibleMatches = new HashMap<String, Match<E>>();
      for(String key : matches.keySet())
      {
         if(matches.get(key).getPattern().matcher(match).matches())
         {
            //noinspection unchecked
            possibleMatches.put(key, matches.get(key));
         }
      }
      return possibleMatches;
   }


   private E getActualMatch(String match, HashMap<String, Match<E>> possibleMatches)
   {
      E value = null;
      Match<E> currentVal = null;
      for(String key : possibleMatches.keySet())
      {
         currentVal = compareMatches(match, currentVal, possibleMatches.get(key));
      }
      if(currentVal != null)
      {
         value = currentVal.getValue();
      }
      return value;
   }

   private Match<E> compareMatches(String match, Match<E> currentVal, Match<E> replacementVal)
   {
      boolean moreSpecific = false;
      if(currentVal == null)
      {
         moreSpecific = true;
      }
      else
      {
         String[] parts = match.split("\\.");
         for(int i = 0; i < parts.length; i++)
         {
            String left = getPart(i, currentVal.getMatch());
            String right = getPart(i, replacementVal.getMatch());
            if(!left.equals(right) && parts[i].equals(right))
            {
               moreSpecific = true;
               if("*".equals(left))
               {
                  break;
               }
            }
            else
            {
               moreSpecific = false;
            }
         }
      }
      return moreSpecific? replacementVal : currentVal;
   }

   private String getPart(int i, String match)
   {
      String[] parts = match.split("\\.");
      if(parts != null &&  parts.length > i)
      {
         return parts[i];
      }
      return null;
   }


}
