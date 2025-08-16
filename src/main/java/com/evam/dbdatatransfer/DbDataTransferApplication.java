package com.evam.dbdatatransfer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DbDataTransferApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(DbDataTransferApplication.class, args)));
    }
}