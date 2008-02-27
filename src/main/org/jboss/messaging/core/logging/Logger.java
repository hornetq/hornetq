package org.jboss.messaging.core.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * A Logger
 * 
 * For now just delegates to org.jboss.util.Logger
 * 
 * This class allows us to isolate all our logging dependencies in one place
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Logger 
{
   private static final ConcurrentMap<Class<?>, Logger> loggers = new ConcurrentHashMap<Class<?>, Logger>();
   
   public static Logger getLogger(final Class<?> clazz)
   {
      Logger logger = loggers.get(clazz);
      
      if (logger == null)
      {
         logger = new Logger(clazz);
         
         Logger oldLogger = loggers.putIfAbsent(clazz, logger);
         
         if (oldLogger != null)
         {
            logger = oldLogger;
         }
      }      
      
      return logger;
   }
   
   private final org.jboss.logging.Logger logger;
   
   private Logger(final Class<?> clazz)
   {
      logger = org.jboss.logging.Logger.getLogger(clazz);
   }
   
   public boolean isInfoEnabled()
   {
      return logger.isInfoEnabled();
   }
   
   public boolean isDebugEnabled()
   {
      return logger.isDebugEnabled();
   }
   
   public boolean isTraceEnabled()
   {
      return logger.isTraceEnabled();
   }
   
   public void fatal(final Object message)
   {
      logger.fatal(message);
   }
   
   public void fatal(final Object message, final Throwable t)
   {
      logger.fatal(message, t);
   }
   
   public void error(final Object message)
   {
      logger.error(message);
   }
   
   public void error(final Object message, final Throwable t)
   {
      logger.error(message, t);
   }
   
   public void warn(final Object message)
   {
      logger.warn(message);
   }
   
   public void warn(final Object message, final Throwable t)
   {
      logger.warn(message, t);
   }
   
   public void info(final Object message)
   {
      logger.info(message);
   }
   
   public void info(final Object message, final Throwable t)
   {
      logger.info(message, t);
   }
   
   public void debug(final Object message)
   {
      logger.debug(message);
   }
   
   public void debug(final Object message, final Throwable t)
   {
      logger.debug(message, t);
   }
   
   public void trace(final Object message)
   {
      logger.trace(message);
   }
   
   public void trace(final Object message, final Throwable t)
   {
      logger.trace(message, t);
   }
   
}
