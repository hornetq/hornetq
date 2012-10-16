/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.settings.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.HierarchicalRepositoryChangeListener;
import org.hornetq.core.settings.Mergeable;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class HierarchicalObjectRepository<T> implements HierarchicalRepository<T>
{

   /**
    * The default Match to fall back to
    */
   private T defaultmatch;

   /**
    * all the matches
    */
   private final Map<String, Match<T>> matches = new HashMap<String, Match<T>>();

   /**
    * Certain values cannot be removed after installed.
    * This is because we read a few records from the main config.
    * JBoss AS deployer may remove them on undeploy, while we don't want to accept that since
    * this could cause issues on shutdown.
    * Notice you can still change these values. You just can't remove them.
    */
   private final Set<String> immutables = new HashSet<String>();

   /**
    * a regex comparator
    */
   private final MatchComparator matchComparator = new MatchComparator();

   /**
    * a cache
    */
   private final Map<String, T> cache = new ConcurrentHashMap<String, T>();

   /**
    * any registered listeners, these get fired on changes to the repository
    */
   private final ArrayList<HierarchicalRepositoryChangeListener> listeners = new ArrayList<HierarchicalRepositoryChangeListener>();


   public void addMatch(final String match, final T value)
   {
      addMatch(match, value, false);
   }


   /**
    * Add a new match to the repository
    *
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
   public void addMatch(final String match, final T value, final boolean immutableMatch)
   {
      clearCache();
      if (immutableMatch)
      {
         immutables.add(match);
      }
      Match.verify(match);
      Match<T> match1 = new Match<T>(match);
      match1.setValue(value);
      matches.put(match, match1);
      onChange();
   }

   public int getCacheSize()
   {
      return cache.size();
   }

   /**
    * return the value held against the nearest match
    *
    * @param match the match to look for
    * @return the value
    */
   public T getMatch(final String match)
   {
      T cacheResult = cache.get(match);
      if (cacheResult != null)
      {
         return cacheResult;
      }
      T actualMatch;
      HashMap<String, Match<T>> possibleMatches = getPossibleMatches(match);
      List<Match<T>> orderedMatches = sort(possibleMatches);
      actualMatch = merge(orderedMatches);
      T value = actualMatch != null ? actualMatch : defaultmatch;
      if (value != null)
      {
         cache.put(match, value);
      }
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
            if (!Mergeable.class.isAssignableFrom(actualMatch.getClass()))
            {
               break;
            }
         }
         else
         {
            ((Mergeable)actualMatch).merge(match.getValue());

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
      boolean isImmutable = immutables.contains(match);
      if (isImmutable)
      {
         HornetQServerLogger.LOGGER.debug("Cannot remove match "  + match + " since it came from a main config");
      }
      else
      {
         matches.remove(match);
         clearCache();
         onChange();
      }
   }

   public void registerListener(final HierarchicalRepositoryChangeListener listener)
   {
      listeners.add(listener);
   }

   public void unRegisterListener(final HierarchicalRepositoryChangeListener listener)
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
      clearCache();
      defaultmatch = defaultValue;
   }

   public void clear()
   {
      clearCache();
      listeners.clear();
      matches.clear();
   }

   public void clearListeners()
   {
      listeners.clear();
   }

   public void clearCache()
   {
      cache.clear();
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
            HornetQServerLogger.LOGGER.errorCallingRepoListener(e);
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
   private static class MatchComparator implements Comparator<String>
   {
      public int compare(final String o1, final String o2)
      {
         if (o1.contains(Match.WILDCARD) && !o2.contains(Match.WILDCARD))
         {
            return +1;
         }
         else if (!o1.contains(Match.WILDCARD) && o2.contains(Match.WILDCARD))
         {
            return -1;
         }
         else if (o1.contains(Match.WILDCARD) && o2.contains(Match.WILDCARD))
         {
            return o2.length() - o1.length();
         }
         else if (o1.contains(Match.WORD_WILDCARD) && !o2.contains(Match.WORD_WILDCARD))
         {
            return +1;
         }
         else if (!o1.contains(Match.WORD_WILDCARD) && o2.contains(Match.WORD_WILDCARD))
         {
            return -1;
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
                     return -1;
                  }
                  else
                  {
                     return +1;
                  }
               }
            }
         }
         return o1.length() - o2.length();
      }
   }
}
