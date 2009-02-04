using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using JSMClient.util;

namespace JMSClientTest.util
{
    /// <summary>
    /// Summary description for SimpleStringTest
    /// </summary>
    [TestFixture]
    public class SimpleStringTest : JBMTest
    {
        public SimpleStringTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }


        [Test]
        public void testString()
        {
            string str = "hello123ABC__524`16254`6125!%^$!%$!%$!%$!%!$%!$$!\uA324";

            SimpleString s = new SimpleString(str);

            assertEquals(str, s.ToString());

            assertEquals(2 * str.Length, s.Data.Length);

            byte[] data = s.Data;

            SimpleString s2 = new SimpleString(data);

            assertEquals(str, s2.ToString());
        }

        [Test]
        public void testStartsWith()
        {
            SimpleString s1 = new SimpleString("abcdefghi");

            assertTrue(s1.StartsWith(new SimpleString("abc")));

            assertTrue(s1.StartsWith(new SimpleString("abcdef")));

            assertTrue(s1.StartsWith(new SimpleString("abcdefghi")));

            assertFalse(s1.StartsWith(new SimpleString("abcdefghijklmn")));

            assertFalse(s1.StartsWith(new SimpleString("aardvark")));

            assertFalse(s1.StartsWith(new SimpleString("z")));
        }

        [Test]
        public void testCharSequence()
        {
            string s = "abcdefghijkl";
            SimpleString s1 = new SimpleString(s);

            assertEquals('a', s1.CharAt(0));
            assertEquals('b', s1.CharAt(1));
            assertEquals('c', s1.CharAt(2));
            assertEquals('k', s1.CharAt(10));
            assertEquals('l', s1.CharAt(11));

            try
            {
                s1.CharAt(-1);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.CharAt(-2);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.CharAt(s.Length);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.CharAt(s.Length + 1);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            assertEquals(s.Length, s1.Length);

            SimpleString ss = s1.SubSequence(0, s1.Length);

            assertEquals(ss, s1);

            ss = s1.SubSequence(1, 4);
            assertEquals(ss, new SimpleString("bcd"));

            ss = s1.SubSequence(5, 10);
            assertEquals(ss, new SimpleString("fghij"));

            ss = s1.SubSequence(5, 12);
            assertEquals(ss, new SimpleString("fghijkl"));

            try
            {
                s1.SubSequence(-1, 2);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.SubSequence(-4, -2);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.SubSequence(0, s1.Length + 1);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.SubSequence(0, s1.Length + 2);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }

            try
            {
                s1.SubSequence(5, 1);
                fail("Should throw exception");
            }
            catch (IndexOutOfRangeException)
            {
                //OK
            }
        }

        [Test]
        public void testEquals()
        {
            assertFalse(new SimpleString("abcdef").Equals(new Object()));

            assertEquals(new SimpleString("abcdef"), new SimpleString("abcdef"));

            assertFalse(new SimpleString("abcdef").Equals(new SimpleString("abggcdef")));
            assertFalse(new SimpleString("abcdef").Equals(new SimpleString("ghijkl")));
        }

        [Test]
        public void testHashcode()
        {
            SimpleString str = new SimpleString("abcdef");
            SimpleString sameStr = new SimpleString("abcdef");
            SimpleString differentStr = new SimpleString("ghijk");

            string tst = str.ToString();
            tst.GetHashCode();

            assertTrue(str.GetHashCode() == sameStr.GetHashCode());
            assertFalse(str.GetHashCode() == differentStr.GetHashCode());

        }

        [Test]
        public void testUnicode()
        {
            String myString = "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

            SimpleString s = new SimpleString(myString);
            byte[] data = s.Data;
            s = new SimpleString(data);

            assertEquals(myString, s.ToString());
        }

        [Test]
        public void testUnicodeWithSurrogates()
        {
            String myString = "abcdef&^*&!^ghijkl\uD900\uDD00";

            SimpleString s = new SimpleString(myString);
            byte[] data = s.Data;
            s = new SimpleString(data);

            assertEquals(myString, s.ToString());
        }

        [Test]
        public void testSizeofString()
        {
            assertEquals(DataConstants.SIZE_INT, SimpleString.SizeofString(new SimpleString("")));

            //FIXME: provide a randomString method
            SimpleString str = new SimpleString(randomString());
            assertEquals(DataConstants.SIZE_INT + str.Data.Length, SimpleString.SizeofString(str));
        }

        [Test]
        public void testSizeofNullableString()
        {
            assertEquals(1, SimpleString.SizeofNullableString(null));

            assertEquals(1 + DataConstants.SIZE_INT, SimpleString.SizeofNullableString(new SimpleString("")));

            SimpleString str = new SimpleString(randomString());
            assertEquals(1 + DataConstants.SIZE_INT + str.Data.Length, SimpleString.SizeofNullableString(str));
        }

        [Test]
        public void testSplitNoDelimeter()
        {
            SimpleString s = new SimpleString("abcdefghi");
            SimpleString[] strings = s.Split('.');
            assertNotNull(strings);
            assertEquals(strings.Length, 1);
            assertEquals(strings[0], s);
        }

        [Test]
        public void testSplit1Delimeter()
        {
            SimpleString s = new SimpleString("abcd.efghi");
            SimpleString[] strings = s.Split('.');
            assertNotNull(strings);
            assertEquals(strings.Length, 2);
            assertEquals(strings[0], new SimpleString("abcd"));
            assertEquals(strings[1], new SimpleString("efghi"));
        }

        [Test]
        public void testSplitmanyDelimeters()
        {
            SimpleString s = new SimpleString("abcd.efghi.jklmn.opqrs.tuvw.xyz");
            SimpleString[] strings = s.Split('.');
            assertNotNull(strings);
            assertEquals(strings.Length, 6);
            assertEquals(strings[0], new SimpleString("abcd"));
            assertEquals(strings[1], new SimpleString("efghi"));
            assertEquals(strings[2], new SimpleString("jklmn"));
            assertEquals(strings[3], new SimpleString("opqrs"));
            assertEquals(strings[4], new SimpleString("tuvw"));
            assertEquals(strings[5], new SimpleString("xyz"));
        }

        [Test]
        public void testContains()
        {
            SimpleString simpleString = new SimpleString("abcdefghijklmnopqrst");
            assertFalse(simpleString.Contains('.'));
            assertFalse(simpleString.Contains('%'));
            assertFalse(simpleString.Contains('8'));
            assertFalse(simpleString.Contains('.'));
            assertTrue(simpleString.Contains('a'));
            assertTrue(simpleString.Contains('b'));
            assertTrue(simpleString.Contains('c'));
            assertTrue(simpleString.Contains('d'));
            assertTrue(simpleString.Contains('e'));
            assertTrue(simpleString.Contains('f'));
            assertTrue(simpleString.Contains('g'));
            assertTrue(simpleString.Contains('h'));
            assertTrue(simpleString.Contains('i'));
            assertTrue(simpleString.Contains('j'));
            assertTrue(simpleString.Contains('k'));
            assertTrue(simpleString.Contains('l'));
            assertTrue(simpleString.Contains('m'));
            assertTrue(simpleString.Contains('n'));
            assertTrue(simpleString.Contains('o'));
            assertTrue(simpleString.Contains('p'));
            assertTrue(simpleString.Contains('q'));
            assertTrue(simpleString.Contains('r'));
            assertTrue(simpleString.Contains('s'));
            assertTrue(simpleString.Contains('t'));
        }

        [Test]
        public void testConcat()
        {
            SimpleString start = new SimpleString("abcdefg");
            SimpleString middle = new SimpleString("hijklmnop");
            SimpleString end = new SimpleString("qrstuvwxyz");
            assertEquals(start.Concat(middle).Concat(end), new SimpleString("abcdefghijklmnopqrstuvwxyz"));
            assertEquals(start.Concat('.').Concat(end), new SimpleString("abcdefg.qrstuvwxyz"));
        }

        [Test]
        public void testConcat33()
        {
            SimpleString start = new SimpleString("a");
            SimpleString middle = new SimpleString("b");
            assertEquals(start.Concat(middle).ToString(), new SimpleString("ab").ToString());
        }
    }

}
