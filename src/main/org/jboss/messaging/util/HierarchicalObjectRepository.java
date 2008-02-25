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

import org.jboss.messaging.core.Mergeable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.reflect.ParameterizedType;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HierarchicalObjectRepository<T> implements HierarchicalRepository<T>
{
   /**
    * The default Match to fall back to
    */
   T defaultmatch;

   /**
    * all the matches
    */
   HashMap<String, Match<T>> matches = new HashMap<String, Match<T>>();

   /**
    * a regex comparator
    */
   MatchComparator<String> matchComparator = new MatchComparator<String>();

   /**
    * a cache
    */
   Map<String, T> cache = new ConcurrentHashMap<String,T>();
   /**
    * Add a new match to the repository
    *
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
   public void addMatch(String match, T value)
   {
      cache.clear();
      Match.verify(match);
      Match<T> match1 = new Match<T>(match);
      match1.setValue(value);
      matches.put(match, match1);

   }

   /**
    * return the value held against the nearest match
    *
    * @param match the match to look for
    * @return the value
    */
   public T getMatch(String match)
   {
      if(cache.get(match) != null)
      {
         return cache.get(match);
      }
      T actualMatch;
      HashMap<String, Match<T>> possibleMatches = getPossibleMatches(match);
      List<Match<T>> orderedMatches = sort(possibleMatches);
      actualMatch = merge(orderedMatches);
      T value = actualMatch != null ? actualMatch : defaultmatch;
      if(value != null)
         cache.put(match, value);
      return value;
   }

   /**
    * merge all the possible matches, if  the values implement Mergeable then a full merge is done
    * @param orderedMatches
    * @return
    */
   private T merge(List<Match<T>> orderedMatches)
   {
      T actualMatch = null;
      for (Match<T> match : orderedMatches)
      {
         if (actualMatch == null || !Mergeable.class.isAssignableFrom(actualMatch.getClass()))
         {
            actualMatch = match.getValue();
         }
         else
         {
            ((Mergeable) actualMatch).merge(match.getValue());

         }
      }
      return actualMatch;
   }

   /**
    * sort the matches in order of precedence
    * @param possibleMatches
    * @return
    */
   private List<Match<T>> sort(HashMap<String, Match<T>> possibleMatches)
   {
      List<String> keys = new ArrayList<String>(possibleMatches.keySet());
      Collections.sort(keys, matchComparator);
      List<Match<T>> matches = new ArrayList<Match<T>>();
      for (String key : keys)
      {
         matches.add(possibleMatches.get(key));
      }
      return matches;
   }

   /**
    * remove a match from the repository
    *
    * @param match the match to remove
    */
   public void removeMatch(String match)
   {
      matches.remove(match);
   }

   /**
    * set the default value to fallback to if none found
    *
    * @param defaultValue the value
    */
   public void setDefault(T defaultValue)
   {
      cache.clear();
      defaultmatch = defaultValue;
   }

   /**
    * return any possible matches
    * @param match
    * @return
    */
   private HashMap<String, Match<T>> getPossibleMatches(String match)
   {
      HashMap<String, Match<T>> possibleMatches = new HashMap<String, Match<T>>();
      for (String key : matches.keySet())
      {
         if (matches.get(key).getPattern().matcher(match).matches())
         {
            possibleMatches.put(key, matches.get(key));
         }
      }
      return possibleMatches;
   }

   /**
    * compares to matches to seew hich one is more specific
    */
   class MatchComparator<T extends String> implements Comparator<T>
   {

      public int compare(String o1, String o2)
      {
         if (o1.contains(Match.WILDCARD) && !o2.contains(Match.WILDCARD))
         {
            return -1;
         }
         else if (!o1.contains(Match.WILDCARD) && o2.contains(Match.WILDCARD))
         {
            return +1;
         }
         else if (o1.contains(Match.WILDCARD) && o2.contains(Match.WILDCARD))
         {
            return o1.length() - o2.length();
         }
         else if (o1.contains(Match.WORD_WILDCARD) && !o2.contains(Match.WORD_WILDCARD))
         {
            return -1;
         }
         else if (!o1.contains(Match.WORD_WILDCARD) && o2.contains(Match.WORD_WILDCARD))
         {
            return +1;
         }
         else if (o1.contains(Match.WORD_WILDCARD) && o2.contains(Match.WORD_WILDCARD))
         {
            String[] leftSplits = o1.split("\\.");
            String[] rightSplits = o2.split("\\.");
            for (int i = 0; i < leftSplits.length; i++)
            {
               String left = leftSplits[i];
               if (left.equals(Match.WORD_WILDCARD))
               {
                  if (rightSplits.length < i || !rightSplits[i].equals(Match.WORD_WILDCARD))
                  {
                     return +1;
                  }
                  else
                  {
                     return -1;
                  }
               }
            }
         }
         return o1.length() - o2.length();
      }
   }
}
