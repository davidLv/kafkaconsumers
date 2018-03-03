package com.cognizant.ddhkafka;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String topic="xyz";
        String fileName = new SimpleDateFormat("yyyyMMddHHmmss'.txt'").format(new Date());
        String fileName1 = new SimpleDateFormat("'topic''_'yyyyMMddHHmmss").format(new Date());
        System.out.println(fileName);
        System.out.println(fileName1);
    }
}
