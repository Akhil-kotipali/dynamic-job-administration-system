package com.akhil.dynamicjobsystem;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DynamicjobsystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(DynamicjobsystemApplication.class, args);
	}

}
