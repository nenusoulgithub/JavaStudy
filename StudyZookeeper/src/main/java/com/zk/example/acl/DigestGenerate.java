package com.zk.example.acl;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.security.NoSuchAlgorithmException;

/**
 * Created by hadoop on 2017/6/30.
 */
public class DigestGenerate {
    public static void main(String[] args) {
        try {
            System.out.println(DigestAuthenticationProvider.generateDigest("zk-client-node4:111111"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
