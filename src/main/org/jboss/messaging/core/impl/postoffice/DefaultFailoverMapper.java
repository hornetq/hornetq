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
package org.jboss.messaging.core.impl.postoffice;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.contract.FailoverMapper;


/**
 * A DefaultFailoverMapper
 * 
 * Generates the mapping by looking to the element to the right and wrapping around to the first
 * element in the list
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2681 $</tt>
 *
 * $Id: DefaultFailoverMapper.java 2681 2007-05-15 00:09:10Z timfox $
 *
 */
public class DefaultFailoverMapper implements FailoverMapper
{
   /**
    * Generate a mapping given a set of nodes - nodes will be sorted by the method.
    *
    * @see org.jboss.messaging.core.contract.FailoverMapper#generateMapping(java.util.Set)
    */
   public Map generateMapping(Set nodes)
   {
      Integer[] nodesArr = (Integer[])nodes.toArray(new Integer[nodes.size()]);
      
      // First sort them so every node has a consistent view
      Arrays.sort(nodesArr);
      
      int s = nodes.size();
      
      // There is no need for the map to be linked
      Map failoverNodes = new HashMap(s);
      
      for (int i = 0; i < s; i++)
      {
         int j = i + 1;
         
         if (j == s)
         {
            j = 0;
         }
         
         failoverNodes.put(nodesArr[i], nodesArr[j]);
      }

      return failoverNodes;
   }

}
