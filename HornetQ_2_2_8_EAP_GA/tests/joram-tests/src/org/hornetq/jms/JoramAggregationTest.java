/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.jms;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.admin.Admin;
import org.objectweb.jtests.jms.admin.AdminFactory;
import org.objectweb.jtests.jms.conform.connection.ConnectionTest;
import org.objectweb.jtests.jms.conform.connection.TopicConnectionTest;
import org.objectweb.jtests.jms.conform.message.MessageBodyTest;
import org.objectweb.jtests.jms.conform.message.MessageDefaultTest;
import org.objectweb.jtests.jms.conform.message.MessageTypeTest;
import org.objectweb.jtests.jms.conform.message.headers.MessageHeaderTest;
import org.objectweb.jtests.jms.conform.message.properties.JMSXPropertyTest;
import org.objectweb.jtests.jms.conform.message.properties.MessagePropertyConversionTest;
import org.objectweb.jtests.jms.conform.message.properties.MessagePropertyTest;
import org.objectweb.jtests.jms.conform.queue.QueueBrowserTest;
import org.objectweb.jtests.jms.conform.queue.TemporaryQueueTest;
import org.objectweb.jtests.jms.conform.selector.SelectorSyntaxTest;
import org.objectweb.jtests.jms.conform.selector.SelectorTest;
import org.objectweb.jtests.jms.conform.session.QueueSessionTest;
import org.objectweb.jtests.jms.conform.session.SessionTest;
import org.objectweb.jtests.jms.conform.session.TopicSessionTest;
import org.objectweb.jtests.jms.conform.session.UnifiedSessionTest;
import org.objectweb.jtests.jms.conform.topic.TemporaryTopicTest;
import org.objectweb.jtests.jms.framework.JMSTestCase;

/**
 * JoramAggregationTest.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @version $Revision: 1.2 $
 */
public class JoramAggregationTest extends TestCase
{
   public JoramAggregationTest(String name)
   {
      super(name);
   }

   
   
   /** Used to similuate tests while renaming its names. */
   private static class DummyTestCase extends TestCase
   {
       DummyTestCase(String name)
       {
           super (name);
       }
   }
 
   /**
    * One of the goals of this class also is to keep original classNames into testNames. So, you will realize several proxies existent here to
    * keep these class names while executing method names.
    */
   static class TestProxy extends TestCase
   {
       Hashtable hashTests = new Hashtable();


       public TestProxy(Test testcase, String name)
       {
           super(name);
           this.testcase = testcase;
       }

       public int countTestCases()
       {
           return testcase.countTestCases();
       }

       /**
        * Create a dummy test renaming its content
        * @param test
        * @return
        */
       private Test createDummyTest(Test test)
       {
           Test dummyTest = (Test)hashTests.get(test);
           if (dummyTest==null)
           {
               if (test instanceof TestCase)
               {
                   dummyTest = new DummyTestCase(this.getName() + ":"+ ((TestCase)test).getName());
               } else
               if (test instanceof TestSuite)
               {
                   dummyTest = new DummyTestCase(this.getName() + ":"+ ((TestCase)test).getName());
               }
               else
               {
                   dummyTest = new DummyTestCase(test.getClass().getName());
               }

               hashTests.put(test,dummyTest);
           }

           return dummyTest;
       }

       public void run(final TestResult result)
       {
           TestResult subResult = new TestResult();
           subResult.addListener(new TestListener()
           {
               public void addError(Test subtest, Throwable throwable)
               {
                   Test dummyTest = createDummyTest(subtest);
                   result.addError(dummyTest, throwable);
               }

               public void addFailure(Test subtest, AssertionFailedError assertionFailedError)
               {
                   Test dummyTest = createDummyTest(subtest);
                   result.addFailure(dummyTest, assertionFailedError);
               }

               public void endTest(Test subtest)
               {
                   Test dummyTest = createDummyTest(subtest);
                   result.endTest(dummyTest);
               }

               public void startTest(Test subtest)
               {
                   Test dummyTest = createDummyTest(subtest);
                   result.startTest(dummyTest);
               }
           });
           testcase.run(subResult);
       }

       Test testcase;
   }

   

   

   public static junit.framework.Test suite() throws Exception
   {
      TestSuite suite = new TestSuite();

      suite.addTest(new TestProxy(TopicConnectionTest.suite(),TopicConnectionTest.class.getName()));
      suite.addTest(new TestProxy(ConnectionTest.suite(), ConnectionTest.class.getName()));
      suite.addTest(new TestProxy(MessageBodyTest.suite(), MessageBodyTest.class.getName()));
      suite.addTest(new TestProxy(MessageDefaultTest.suite(), MessageDefaultTest.class.getName()));
      suite.addTest(new TestProxy(MessageTypeTest.suite(), MessageTypeTest.class.getName()));
      suite.addTest(new TestProxy(MessageHeaderTest.suite(), MessageHeaderTest.class.getName()));
      suite.addTest(new TestProxy(JMSXPropertyTest.suite(), JMSXPropertyTest.class.getName()));
      suite.addTest(new TestProxy(MessagePropertyConversionTest.suite(), MessagePropertyConversionTest.class.getName()));
      suite.addTest(new TestProxy(MessagePropertyTest.suite(), MessagePropertyTest.class.getName()));
      suite.addTest(new TestProxy(QueueBrowserTest.suite(), QueueBrowserTest.class.getName()));
      suite.addTest(new TestProxy(TemporaryQueueTest.suite(), TemporaryQueueTest.class.getName()));
      suite.addTest(new TestProxy(SelectorSyntaxTest.suite(), SelectorSyntaxTest.class.getName()));
      suite.addTest(new TestProxy(SelectorTest.suite(), SelectorTest.class.getName()));
      suite.addTest(new TestProxy(QueueSessionTest.suite(), QueueSessionTest.class.getName()));
      suite.addTest(new TestProxy(SessionTest.suite(), SessionTest.class.getName()));
      suite.addTest(new TestProxy(TopicSessionTest.suite(), TopicSessionTest.class.getName()));
      suite.addTest(new TestProxy(UnifiedSessionTest.suite(), UnifiedSessionTest.class.getName()));
      suite.addTest(new TestProxy(TemporaryTopicTest.suite(), TemporaryTopicTest.class.getName()));
      
      return new TestAggregation(suite);
   }
   /**
    * Should be overriden 
    * @return
    */
   protected static Properties getProviderProperties() throws IOException
   {
      Properties props = new Properties();
      props.load(ClassLoader.getSystemResourceAsStream(JMSTestCase.PROP_FILE_NAME));
      return props;
   }

   
   static class TestAggregation extends TestSetup
   {
      
      Admin admin;

      /**
       * @param test
       */
      public TestAggregation(Test test)
      {
         super(test);
      }
      
      public void setUp() throws Exception
      {
         JMSTestCase.startServer = false;
         // Admin step
         // gets the provider administration wrapper...
         Properties props = getProviderProperties();
         admin = AdminFactory.getAdmin(props);
         admin.startServer();

      }
      
      public void tearDown() throws Exception
      {
         System.out.println("TearDown");
         admin.stopServer();
         JMSTestCase.startServer = true;
      }
      
   }
}
