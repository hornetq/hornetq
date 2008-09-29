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

import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.ArrayList;
import java.util.List;

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
      
      Consumer c = dp.select(null, false);
      
      assertEquals(null, c);
   }
   
   public void testConsumers()
   {
      FakeConsumer c1 = new FakeConsumer();
      FakeConsumer c2 = new FakeConsumer();
      FakeConsumer c3 = new FakeConsumer();
      
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
            
      Consumer c = null;
      
      c = dp.select( null, false);
      
      assertEquals(c1, c);
      
      c = dp.select(null, false);
      
      assertEquals(c2, c);
      
      c = dp.select(null, false);
      
      assertEquals(c3, c);
      
      c = dp.select( null, false);
      
      assertEquals(c1, c);
      
      c = dp.select( null, false);
      
      assertEquals(c2, c);
      
      c = dp.select( null, false);
      
      assertEquals(c3, c);
      
      c = dp.select(null, false);
      
      assertEquals(c1, c);
   }
   
}
