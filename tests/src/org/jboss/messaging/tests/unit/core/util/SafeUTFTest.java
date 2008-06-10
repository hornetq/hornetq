/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.tests.unit.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.jboss.messaging.util.SafeUTF;

/**
 * 
 * There is a bug in JDK1.3, 1.4 whereby writeUTF fails if more than 64K bytes are written
 * we need to work with all size of strings
 * 
 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4806007
 * http://jira.jboss.com/jira/browse/JBAS-2641
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version $Revision: 1174 $
 */
public class SafeUTFTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SafeUTFTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------
   
   public void testSafeWriteReadUTFTest1() throws Exception
   {
      //Make sure we hit all the edge cases
      doTest("a", 10);
      doTest("ab", 10);
      doTest("abc", 10);
      doTest("abcd", 10);
      doTest("abcde", 10);
      doTest("abcdef", 10);
      doTest("abcdefg", 10);
      doTest("abcdefgh", 10);
      doTest("abcdefghi", 10);
      doTest("abcdefghij", 10);
      doTest("abcdefghijk", 10);
      doTest("abcdefghijkl", 10);
      doTest("abcdefghijklm", 10);
      doTest("abcdefghijklmn", 10);
      doTest("abcdefghijklmno", 10);
      doTest("abcdefghijklmnop", 10);
      doTest("abcdefghijklmnopq", 10);
      doTest("abcdefghijklmnopqr", 10);
      doTest("abcdefghijklmnopqrs", 10);
      doTest("abcdefghijklmnopqrst", 10);
      doTest("abcdefghijklmnopqrstu", 10);
      doTest("abcdefghijklmnopqrstuv", 10);
      doTest("abcdefghijklmnopqrstuvw", 10);
      doTest("abcdefghijklmnopqrstuvwx", 10);
      doTest("abcdefghijklmnopqrstuvwxy", 10);
      doTest("abcdefghijklmnopqrstuvwxyz", 10);
      
      //Need to work with null too
      doTest(null, 10);
              
   }
   
   protected void doTest(String s, int chunkSize) throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
      DataOutputStream dos = new DataOutputStream(bos);
      
      SafeUTF.safeWriteUTF(dos, s);
      
      dos.close();
      
      byte[] bytes = bos.toByteArray();
      
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      DataInputStream dis = new DataInputStream(bis);
      
      String s2 = SafeUTF.safeReadUTF(dis);
      
      assertEquals(s, s2);   
   }
   
   public void testSafeWriteReadUTFTest2() throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
      DataOutputStream dos = new DataOutputStream(bos);
      
      String s = "abcdefghijklmnopqrstuvwxyz";
      
      SafeUTF.safeWriteUTF(dos, s);
      
      dos.close();
      
      byte[] bytes = bos.toByteArray();
      
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      DataInputStream dis = new DataInputStream(bis);
      
      String s2 = SafeUTF.safeReadUTF(dis);
      
      assertEquals(s, s2); 
   }
         
   protected String genString(int len)
   {
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = (char)(65 + i % 26);
      }
      return new String(chars);
   }
}
