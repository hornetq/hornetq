/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.settings;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface HierarchicalRepository<T>
{
   /**
    * Add a new match to the repository
    * @param match The regex to use to match against
    * @param value the value to hold agains the match
    */
    void addMatch(String match, T value);

   /**
    * return the value held against the nearest match
    * @param match the match to look for
    * @return the value
    */
   T getMatch(String match);

   /**
    * set the default value to fallback to if none found
    * @param defaultValue the value
    */
   void setDefault(T defaultValue);

   /**
    * remove a match from the repository
    * @param match the match to remove
    */
   void removeMatch(String match);


   /**
    * register a listener to listen for changes in the repository
    * @param listener
    */
   void registerListener(HierarchicalRepositoryChangeListener listener);

   /**
    * unregister a listener
    * @param listener
    */
   void unRegisterListener(HierarchicalRepositoryChangeListener listener);
}
