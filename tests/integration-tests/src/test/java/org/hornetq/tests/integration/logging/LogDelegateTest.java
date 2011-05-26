/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.tests.integration.logging;

import junit.framework.Assert;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.logging.impl.JULLogDelegate;
import org.hornetq.core.logging.impl.JULLogDelegateFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.logging.Log4jLogDelegate;
import org.hornetq.integration.logging.Log4jLogDelegateFactory;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A LogDelegateTest
 *
 * @author Tim Fox
 *
 *
 */
public class LogDelegateTest extends ServiceTestBase
{
   public void testConfigureJULViaConfiguration() throws Exception
   {
      Configuration config = createBasicConfig();

      String className = JULLogDelegateFactory.class.getCanonicalName();

      config.setLogDelegateFactoryClassName(className);

      HornetQServer server = super.createServer(false, config);

      server.start();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof JULLogDelegate);

      server.stop();
   }

   public void testConfigureLog4jViaConfiguration() throws Exception
   {
      Configuration config = createBasicConfig();

      String className = Log4jLogDelegateFactory.class.getCanonicalName();

      config.setLogDelegateFactoryClassName(className);

      HornetQServer server = super.createServer(false, config);

      server.start();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof Log4jLogDelegate);

      server.stop();
   }

   public void testConfigureLog4jViaSystemProperty() throws Exception
   {
      System.setProperty(Logger.LOGGER_DELEGATE_FACTORY_CLASS_NAME, Log4jLogDelegateFactory.class.getCanonicalName());

      Logger.reset();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof Log4jLogDelegate);
   }

   public void testConfigureJULViaSystemProperty() throws Exception
   {
      System.setProperty(Logger.LOGGER_DELEGATE_FACTORY_CLASS_NAME, JULLogDelegateFactory.class.getCanonicalName());

      Logger.reset();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof JULLogDelegate);
   }

   public void testDefault() throws Exception
   {
      Logger.reset();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof JULLogDelegate);
   }

   public void testDefaultWithConfiguration() throws Exception
   {
      Configuration config = createBasicConfig();

      HornetQServer server = super.createServer(false, config);

      server.start();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof JULLogDelegate);

      server.stop();
   }

   public void testUserDefinedLogger() throws Exception
   {
      Configuration config = createBasicConfig();

      String className = MyLogDelegateFactory.class.getCanonicalName();

      config.setLogDelegateFactoryClassName(className);

      HornetQServer server = super.createServer(false, config);

      server.start();

      Logger logger = Logger.getLogger(this.getClass());

      Assert.assertTrue(logger.getDelegate() instanceof MyLogDelegateFactory.MyLogDelegate);

      server.stop();
   }

}
