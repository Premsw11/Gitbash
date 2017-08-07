package com.cisco.DBConnections;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Reporter;
import org.apache.log4j.xml.DOMConfigurator;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;



public class UseTestNG {
	
	Reporter report = new Reporter();
	public static void main(String[] args) {
		task1();
		task2();
		System.out.println("User directory gives the current project directory  "+System.getProperty("user.dir"));
	}

	@Test
	public static void task1() {
		System.out.println("First task is completed");
		AssertJUnit.assertEquals("Task1 is completed", "Task1 is completed");
	}


	@Test
	public static void task2() {
		System.out.println("Second task is completed");
		AssertJUnit.assertEquals("Task2 is completed", "Task2 is completed");
	}
	

}


