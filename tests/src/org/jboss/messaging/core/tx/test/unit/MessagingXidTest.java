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
package org.jboss.messaging.core.tx.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.test.unit.RandomUtil;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A MessagingXidTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessagingXidTest extends UnitTestCase
{
   public void testSerialize() throws Exception
   {
      MessagingXid xid = new MessagingXid(RandomUtil.randomBytes(), RandomUtil.randomInt(),
                                          RandomUtil.randomBytes());
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      
      oos.writeObject(xid);
      
      oos.flush();
      
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
      
      Object obj = ois.readObject();
      
      assertTrue(obj instanceof MessagingXid);
      
      MessagingXid xid2 = (MessagingXid)obj;
      
      assertXidsEquivalent(xid, xid2);
      
      assertEquals(xid, xid2);     
   }
   
   public void testStandardConstructor()
   {
      byte[] bq = RandomUtil.randomBytes();
      
      byte[] globalTXID = RandomUtil.randomBytes();
      
      int formatID = RandomUtil.randomInt();
      
      MessagingXid xid1 = new MessagingXid(bq, formatID, globalTXID);
      
      assertByteArraysEquivalent(bq, xid1.getBranchQualifier());
      
      assertByteArraysEquivalent(globalTXID, xid1.getGlobalTransactionId());
      
      assertEquals(formatID, xid1.getFormatId());
   }
   
   public void testCopyConstructor()
   {
      MessagingXid xid1 = new MessagingXid(RandomUtil.randomBytes(), RandomUtil.randomInt(),
                                          RandomUtil.randomBytes());
      
      MessagingXid xid2 = new MessagingXid(xid1);
      
      assertXidsEquivalent(xid1, xid2);
      
      assertEquals(xid2, xid2);
   }
   
   public void testDefaultConstructor()
   {
      MessagingXid xid1 = new MessagingXid();
      
      assertNull(xid1.getBranchQualifier());
      
      assertNull(xid1.getGlobalTransactionId());
      
      assertEquals(0, xid1.getFormatId());
   }
   
   public void testEqualsWithForeign()
   {
      MessagingXid xid1 = new MessagingXid(RandomUtil.randomBytes(), RandomUtil.randomInt(),
            RandomUtil.randomBytes());

      Xid foreign = new ForeignXid(xid1.getBranchQualifier(), xid1.getFormatId(), xid1.getGlobalTransactionId());
      
      assertTrue(xid1.equals(foreign));
      
      foreign = new ForeignXid(RandomUtil.randomBytes(), RandomUtil.randomInt(),
                               RandomUtil.randomBytes());
      
      assertFalse(xid1.equals(foreign));
      
   }
   
   // Private ---------------------------------------------------------------------------------
   
   private void assertXidsEquivalent(Xid xid1, Xid xid2)
   {
      assertByteArraysEquivalent(xid1.getBranchQualifier(), xid2.getBranchQualifier());
      
      assertEquals(xid1.getFormatId(), xid2.getFormatId());
      
      assertByteArraysEquivalent(xid1.getGlobalTransactionId(), xid2.getGlobalTransactionId());
   }
   
   // Inner classes ---------------------------------------------------------------------------
   
   class ForeignXid implements Xid
   {
      private byte[] branchQualifier;
      
      private int formatId;
      
      private byte[] globalTransactionId;
      
      public ForeignXid(byte[] branchQualifier, int formatId, byte[] globalTransactionId)
      {
         this.branchQualifier = branchQualifier;
         this.formatId = formatId;
         this.globalTransactionId = globalTransactionId;          
      }
           
      public byte[] getBranchQualifier()
      {
         return this.branchQualifier;
      }

      public int getFormatId()
      {
         return this.formatId;
      }

      public byte[] getGlobalTransactionId()
      {
         return this.globalTransactionId;
      }
      
   }
   
   
}
