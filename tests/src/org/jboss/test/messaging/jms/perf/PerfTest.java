/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import org.jboss.test.messaging.jms.perf.framework.PerfRunner;

import junit.framework.TestCase;

/*
 * JUnit wrapper for the performance tests
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class PerfTest extends TestCase
{
   protected PerfRunner runner;
   
   public void setUp()
   {
      runner = new PerfRunner();
      runner.setUp();
   }
     
   public void tearDown()
   {
      runner.tearDown();
   }
   
   public void testQueue1()
   {
      runner.testQueue1();
   }
   
   public void testQueue2()
   {
      runner.testQueue2();
   }
   
   public void testQueue3()
   {
      runner.testQueue3();
   }
   
   public void testQueue4()
   {
      runner.testQueue4();
   }
   
   public void testQueue5()
   {
      runner.testQueue5();
   }
   
   public void testQueue6()
   {
      runner.testQueue6();
   }
   
   public void testQueue7()
   {
      runner.testQueue7();
   }
   
   public void testQueue8()
   {
      runner.testQueue8();
   }
   
   public void testQueue9()
   {
      runner.testQueue9();
   }
   
   public void testQueue10()
   {
      runner.testQueue10();
   }
   
   public void testQueue11()
   {
      runner.testQueue11();
   }
   
   public void testQueue12()
   {
      runner.testQueue12();
   }
   
   public void testQueue13()
   {
      runner.testQueue13();
   }
   
   public void testQueue14()
   {
      runner.testQueue14();
   }
   
   public void testQueue15()
   {
      runner.testQueue15();
   }
   
   public void testQueue16()
   {
      runner.testQueue16();
   }
   
   public void testQueue17()
   {
      runner.testQueue17();
   }
   
   public void testQueue18()
   {
      runner.testQueue18();
   }
   
   public void testQueue19()
   {
      runner.testQueue19();
   }
   
   public void testQueue20()
   {
      runner.testQueue20();
   }
   
   
   public void testTopic1()
   {
      runner.testTopic1();
   }
   
   public void testTopic2()
   {
      runner.testTopic2();
   }
   
   public void testTopic3()
   {
      runner.testTopic3();
   }
   
   public void testTopic4()
   {
      runner.testTopic4();
   }
   
   public void testTopic5()
   {
      runner.testTopic5();
   }
   
   public void testTopic6()
   {
      runner.testTopic6();
   }
   
   public void testTopic7()
   {
      runner.testTopic7();
   }
   
   public void testTopic8()
   {
      runner.testTopic8();
   }
   
   public void testTopic9()
   {
      runner.testTopic9();
   }
   
   public void testTopic10()
   {
      runner.testTopic10();
   }
   

   
   public void testMessageSizeThroughput()
   {
      runner.testMessageSizeThroughput();
   }
   
   public void testQueueScale1()
   {
      runner.testQueueScale1();
   }
   
   public void testQueueScale2()
   {
      runner.testQueueScale2();
   }
   
   public void testQueueScale3()
   {
      runner.testQueueScale3();
   }

   
   public void testTopicScale1()
   {
      runner.testTopicScale1();
   }
   
   public void testTopicScale2()
   {
      runner.testTopicScale2();
   }
   
   public void testTopicScale3()
   {
      runner.testTopicScale3();
   }

}
