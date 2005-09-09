/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CSVResultPersistor implements ResultPersistor
{
   private transient static final Logger log = Logger.getLogger(CSVResultPersistor.class);
   
   File f;
   
   FileWriter writer;
   
   public CSVResultPersistor(String filename) throws IOException
   {
      f = new File(filename);
      writer = new FileWriter(f);
   }
   
   public void addValue(String fieldName, String value)
   {
      try
      {
         writer.write(value);
         writer.write(",");
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void addValue(String fieldName, long value)
   {
      try
      {
         writer.write(String.valueOf(value));
         writer.write(",");
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void addValue(String fieldName, int value)
   {
      try
      {
         writer.write(String.valueOf(value));
         writer.write(",");      
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void addValue(String fieldName, boolean value)
   {
      try
      {
         writer.write(String.valueOf(value));
         writer.write(",");
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void addValue(String fieldName, double value)
   {
      try
      {
         writer.write(String.valueOf(value));
         writer.write(",");
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void persist()
   {
      try
      {
         writer.write("\n");
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
   
   public void close()
   {
      try
      {
         writer.close();
      }
      catch (IOException e)
      {
         log.error("Failed to write field", e);
      }
   }
}
