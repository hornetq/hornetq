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

package org.hornetq.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestSuite;

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;

/**
 * 
 * A AIOSequentialFileFactoryTest
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFileFactoryTest extends SequentialFileFactoryTestBase
{
   
   public static TestSuite suite()
   {
      return createAIOTestSuite(AIOSequentialFileFactoryTest.class);
   }
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdirs();
   }

   @Override
   protected SequentialFileFactory createFactory()
   {
      return new AIOSequentialFileFactory(getTestDir());
   }

   public void testBuffer() throws Exception
   {
      SequentialFile file = factory.createSequentialFile("filtetmp.log", 10);
      file.open();
      ByteBuffer buff = factory.newBuffer(10);
      assertEquals(512, buff.limit());
      file.close();
      factory.releaseBuffer(buff);
   }

}
