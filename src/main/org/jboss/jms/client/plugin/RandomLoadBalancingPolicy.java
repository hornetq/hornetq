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

package org.jboss.jms.client.plugin;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import java.util.Random;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RandomLoadBalancingPolicy implements LoadBalancingPolicy
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = -4377960504057811985L;
	
   // Attributes -----------------------------------------------------------------------------------

	private ConnectionFactoryDelegate[] delegates;
   private transient Random random = null;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public RandomLoadBalancingPolicy(ConnectionFactoryDelegate[] delegates)
   {
      this.delegates = delegates;
   }

   // Public ---------------------------------------------------------------------------------------

   public synchronized ConnectionFactoryDelegate getNext()
   {
      return delegates[randomSelect()];
   }

   public synchronized void updateView(ConnectionFactoryDelegate[] delegates)
   {
      this.delegates = delegates;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected int randomSelect()
   {
      if (random == null)
      {
         random = new Random();
      }
      int nextInt = random.nextInt() % delegates.length;
      if (nextInt<0)
      {
         nextInt *= -1;
      }

      return nextInt;
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
