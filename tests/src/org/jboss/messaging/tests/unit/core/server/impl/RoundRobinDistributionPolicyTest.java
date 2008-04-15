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
