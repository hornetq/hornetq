/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ResultPersistor
{
   void addValue(String fieldName, String value);
   
   void addValue(String fieldName, long value);
   
   void addValue(String fieldName, int value);
   
   void addValue(String fieldName, boolean value);
   
   void addValue(String fieldName, double value);
   
   void persist();
   
   void close();
}
