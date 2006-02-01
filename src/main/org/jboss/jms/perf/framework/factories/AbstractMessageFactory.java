/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import java.util.Arrays;

/**
 * 
 * A AbstractMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public abstract class AbstractMessageFactory implements MessageFactory
{
   protected static byte[] getBytes(int size)
   {
      byte[] bytes = new byte[size];         
      //Just fill it with something so we can recognise it if we look at it via TCPDump for instance
      Arrays.fill(bytes, (byte)99);  
      return bytes;
   }  
  
}
