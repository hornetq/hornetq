using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace JMSClientTest.util
{
    public class JBMTest
    {
        public void assertTrue(bool value)
        {
            Assert.IsTrue(value);
        }

        public void assertFalse(bool value)
        {
            Assert.IsFalse(value);
        }


        public void assertEquals(Object obj, Object obj2)
        {
            Assert.AreEqual(obj, obj2);
        }

        public void fail(string message)
        {
            Assert.Fail(message);
        }

        public string randomString()
        {
            // FIXME : implement this
            return "implement-me please";
        }

        public void assertNotNull(Object value)
        {
            Assert.IsNotNull(value);
        }
    }
}
