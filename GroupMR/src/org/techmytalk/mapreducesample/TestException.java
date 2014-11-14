package org.techmytalk.mapreducesample;

public class TestException {
    public static void main(String[] args) throws ClassNotFoundException {
        //Class.forName("abc.class");
        TestException abc=new TestException();
        abc.testSubclass();
    }
    
    public void testSubclass(){
        Test2 test1=new Test2();
        test1.test1();
    }

    public class Test2{
        public Test2(){
            
        }
        public void test1(){
            System.out.println("Inside test1");
        }
    }
    
    
}
