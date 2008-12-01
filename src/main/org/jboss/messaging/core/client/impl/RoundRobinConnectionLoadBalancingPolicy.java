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


package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ConnectionLoadBalancingPolicy;
import org.jboss.messaging.util.Random;

/**
 * A RoundRobinConnectionLoadBalancingPolicy
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 28 Nov 2008 10:21:08
 *
 *
 */
public class RoundRobinConnectionLoadBalancingPolicy implements ConnectionLoadBalancingPolicy
{
   private final Random random = new Random();
   
   private boolean first = true;
   
   private int pos;

   public int select(final int max)
   {
      if (first)
      {
         //We start on a random one
         pos = random.getRandom().nextInt(max);
         
         first = false;
      }
      else
      {
         pos++;
         
         if (pos >= max)
         {
            pos = 0;
         }
      }
     
      return pos;
   }  
}
