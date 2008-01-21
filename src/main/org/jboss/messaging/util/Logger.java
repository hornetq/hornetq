package org.jboss.messaging.util;

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
   private static ConcurrentMap<Class, Logger> loggers = new ConcurrentHashMap();
   
   public static Logger getLogger(Class clazz)
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
   
   private org.jboss.logging.Logger logger;
   
   private Logger(Class clazz)
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
   
   public void fatal(Object message)
   {
      logger.fatal(message);
   }
   
   public void fatal(Object message, Throwable t)
   {
      logger.fatal(message, t);
   }
   
   public void error(Object message)
   {
      logger.error(message);
   }
   
   public void error(Object message, Throwable t)
   {
      logger.error(message, t);
   }
   
   public void warn(Object message)
   {
      logger.warn(message);
   }
   
   public void warn(Object message, Throwable t)
   {
      logger.warn(message, t);
   }
   
   public void info(Object message)
   {
      logger.info(message);
   }
   
   public void info(Object message, Throwable t)
   {
      logger.info(message, t);
   }
   
   public void debug(Object message)
   {
      logger.debug(message);
   }
   
   public void debug(Object message, Throwable t)
   {
      logger.debug(message, t);
   }
   
   public void trace(Object message)
   {
      logger.trace(message);
   }
   
   public void trace(Object message, Throwable t)
   {
      logger.trace(message, t);
   }
   
}
