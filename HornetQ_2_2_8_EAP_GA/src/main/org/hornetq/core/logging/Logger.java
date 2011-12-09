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

package org.hornetq.core.logging;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.core.logging.impl.JULLogDelegateFactory;
import org.hornetq.spi.core.logging.LogDelegate;
import org.hornetq.spi.core.logging.LogDelegateFactory;

/**
 * 
 * A Logger
 * 
 * This class allows us to isolate all our logging dependencies in one place
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Logger
{
   public static final String LOGGER_DELEGATE_FACTORY_CLASS_NAME = "org.hornetq.logger-delegate-factory-class-name";

   private static volatile LogDelegateFactory delegateFactory;

   private static final ConcurrentMap<Class<?>, Logger> loggers = new ConcurrentHashMap<Class<?>, Logger>();

   static
   {
      Logger.initialise();
   }

   public static synchronized void setDelegateFactory(final LogDelegateFactory delegateFactory)
   {
      Logger.clear();

      Logger.delegateFactory = delegateFactory;
   }

   private static void clear()
   {
      Logger.loggers.clear();
   }

   public static synchronized void reset()
   {
      Logger.clear();

      Logger.initialise();
   }

   public static synchronized void initialise()
   {
      LogDelegateFactory delegateFactory;

      // If a system property is specified then this overrides any delegate factory which is set
      // programmatically - this is primarily of use so we can configure the logger delegate on the client side.
      // call to System.getProperty is wrapped in a try block as it will fail if the client runs in a secured
      // environment
      String className = JULLogDelegateFactory.class.getName();
      try
      {
         className = System.getProperty(Logger.LOGGER_DELEGATE_FACTORY_CLASS_NAME);
      }
      catch (Exception e)
      {
      }

      if (className != null)
      {
          delegateFactory = (LogDelegateFactory) safeInitNewInstance(className);
      }
      else
      {
         delegateFactory = new JULLogDelegateFactory();
      }

      Logger.delegateFactory = delegateFactory;

      Logger.loggers.clear();
   }

   public static Logger getLogger(final Class<?> clazz)
   {
      Logger logger = Logger.loggers.get(clazz);

      if (logger == null)
      {
         LogDelegate delegate = Logger.delegateFactory.createDelegate(clazz);

         logger = new Logger(delegate);

         Logger oldLogger = Logger.loggers.putIfAbsent(clazz, logger);

         if (oldLogger != null)
         {
            logger = oldLogger;
         }
      }

      return logger;
   }

   private final LogDelegate delegate;

   Logger(final LogDelegate delegate)
   {
      this.delegate = delegate;
   }

   public LogDelegate getDelegate()
   {
      return delegate;
   }

   public boolean isInfoEnabled()
   {
      return delegate.isInfoEnabled();
   }

   public boolean isDebugEnabled()
   {
      return delegate.isDebugEnabled();
   }

   public boolean isTraceEnabled()
   {
      return delegate.isTraceEnabled();
   }

   public void fatal(final Object message)
   {
      delegate.fatal(message);
   }

   public void fatal(final Object message, final Throwable t)
   {
      delegate.fatal(message, t);
   }

   public void error(final Object message)
   {
      delegate.error(message);
   }

   public void error(final Object message, final Throwable t)
   {
      delegate.error(message, t);
   }

   public void warn(final Object message)
   {
      delegate.warn(message);
   }

   public void warn(final Object message, final Throwable t)
   {
      delegate.warn(message, t);
   }

   public void info(final Object message)
   {
      delegate.info(message);
   }

   public void info(final Object message, final Throwable t)
   {
      delegate.info(message, t);
   }

   public void debug(final Object message)
   {
      delegate.debug(message);
   }

   public void debug(final Object message, final Throwable t)
   {
      delegate.debug(message, t);
   }

   public void trace(final Object message)
   {
      delegate.trace(message);
   }

   public void trace(final Object message, final Throwable t)
   {
      delegate.trace(message, t);
   }
   
   /** This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    *  utility class, as it would be a door to load anything you like in a safe VM.
    *  For that reason any class trying to do a privileged block should do with the AccessController directly.
    * @param className
    * @return
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            ClassLoader loader = this.getClass().getClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            }
            catch (Throwable t)
            {
                try
                {
                    loader = Thread.currentThread().getContextClassLoader();
                    if (loader != null)
                        return loader.loadClass(className).newInstance();
                }
                catch (RuntimeException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                }

                throw new IllegalArgumentException("Could not find class " + className);
            }
         }
      });
   }



}
