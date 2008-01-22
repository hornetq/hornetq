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
         Match match1 = new Match(match);
         match1.setValue(value);
         matches.put(match, match1);
      }
   }

   /**
    * return the value held against the nearest match
    * @param match the match to look for
    * @return the value
    */
   //todo implement a better algorithm for returning a match!
   public E getMatch(String match)
   {
      for (Match securityMatch : matches.values())
      {
         if(securityMatch.getPattern().matcher(match).matches())
         {
            //noinspection unchecked
            return (E) securityMatch.getValue();
         }
      }
      return defaultmatch;
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
}
