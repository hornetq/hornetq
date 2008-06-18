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

package org.jboss.messaging.tests.unit.core.server.impl;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A RoundRobinDistributionPolicyTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RoundRobinDistributionPolicyTest extends UnitTestCase
{

   public void testNoConsumers()
   {
      List<Consumer> consumers = new ArrayList<Consumer>();
      
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      
      int pos = dp.select(consumers, -1);
      
      assertEquals(0, pos);
   }
   
   public void testConsumers()
   {
      List<Consumer> consumers = new ArrayList<Consumer>();
      
      consumers.add(new FakeConsumer());
      consumers.add(new FakeConsumer());
      consumers.add(new FakeConsumer());
      
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
            
      int pos = -1;
      
      pos = dp.select(consumers, pos);
      
      assertEquals(0, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(1, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(2, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(0, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(1, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(2, pos);
      
      pos = dp.select(consumers, pos);
      
      assertEquals(0, pos);
   }
   
}
