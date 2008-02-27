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
package org.jboss.messaging.core.settings.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.Mergeable;
import org.jboss.messaging.core.settings.HierarchicalRepositoryChangeListener;
import org.jboss.messaging.core.logging.Logger;


/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class HierarchicalObjectRepository<T> implements HierarchicalRepository<T>
{
   Logger log = Logger.getLogger(HierarchicalObjectRepository.class);
   /**
    * The default Match to fall back to
    */
   private T defaultmatch;

   /**
    * all the matches
    */
   private final Map<String, Match<T>> matches = new HashMap<String, Match<T>>();

   /**
    * a regex comparator
    */
   private final MatchComparator<String> matchComparator = new MatchComparator<String>();

   /**
    * a cache
    */
   private final Map<String, T> cache = new ConcurrentHashMap<String,T>();
   

   /**
    * any registered listeners, these get fired on changes to the repository
    */
   private ArrayList<HierarchicalRepositoryChangeListener> listeners = new ArrayList<HierarchicalRepositoryChangeListener>();

   /**
    * Add a new match to the repository
    *
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
   public void addMatch(final String match, final T value)
   {
      cache.clear();
      Match.verify(match);
      Match<T> match1 = new Match<T>(match);
      match1.setValue(value);
      matches.put(match, match1);
      onChange();
   }

   /**
    * return the value held against the nearest match
    *
    * @param match the match to look for
    * @return the value
    */
   public T getMatch(final String match)
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
   private T merge(final List<Match<T>> orderedMatches)
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
   private List<Match<T>> sort(final Map<String, Match<T>> possibleMatches)
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
   public void removeMatch(final String match)
   {
      matches.remove(match);
      onChange();
   }

   public void registerListener(HierarchicalRepositoryChangeListener listener)
   {
      listeners.add(listener);
   }

   public void unRegisterListener(HierarchicalRepositoryChangeListener listener)
   {
      listeners.remove(listener);
   }

   /**
    * set the default value to fallback to if none found
    *
    * @param defaultValue the value
    */
   public void setDefault(final T defaultValue)
   {
      cache.clear();
      defaultmatch = defaultValue;
   }

   private void onChange()
   {
      for (HierarchicalRepositoryChangeListener listener : listeners)
      {
         try
         {
            listener.onChange();
         }
         catch (Throwable e)
         {
            log.error("Unable to call listener:", e);
         }
      }
   }

   /**
    * return any possible matches
    * @param match
    * @return
    */
   private HashMap<String, Match<T>> getPossibleMatches(final String match)
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
    * compares to matches to see which one is more specific
    */
   private static class MatchComparator<T extends String> implements Comparator<T>
   {
      public int compare(final String o1, final String o2)
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
