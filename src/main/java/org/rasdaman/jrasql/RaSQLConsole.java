/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rasdaman.jrasql;


import rasj.*;
import rasj.odmg.*;
import org.odmg.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.*;

/**
 * A simple console application for sending Rasdaman Query Language queries. 
 * 
 * @author Ulrich Loup <ulrich.loup@dwd.de>
 */
public class RaSQLConsole {
    
    public static String SERVER = "localhost";
    public static String PORT = "7001";
    public static String DATABASE = "RASBASE";
    public static String USER = "rasquest";
    public static String PASSWORD = "rasquest";

    public static void main(String[] args) {
        String server = SERVER;
        String port = PORT;
        String database = DATABASE;
        String user = USER;
        String password = PASSWORD;

        System.out.println("Rasdaman Query Language (RaSQL) Console");
        System.out.println();
        
        /*
         * Parsing arguments
         */
        for (int i = args.length - 1; i >= 0; i--) {
            if (args[i].equals("-help")) {
                printUsage();
                System.exit(0);
            }
            if (args[i].equals("-server")) {
                server = args[i + 1];
                System.out.println("Setting server = " + server);
            }
            if (args[i].equals("-port")) {
                port = args[i + 1];
                System.out.println("Setting port = " + port);
            }
            if (args[i].equals("-database")) {
                database = args[i + 1];
                System.out.println("Setting database = " + database);
            }
            if (args[i].equals("-user")) {
                user = args[i + 1];
                System.out.println("Setting user = " + user);
            }
            if (args[i].equals("-password")) {
                password = args[i + 1];
                System.out.println("Setting password = " + password);
            }
        }
        
        /*
         * Connecting to Rasdaman
         */
        Database databaseConnection = null;
        Implementation application = null;
        try {
            if (!server.startsWith("http://") && server.startsWith("https://")) server = "http://" + server;
            if (server.endsWith("/")) server = server.substring(0, server.length()-1);
            application = new RasImplementation(server + ":" + port);
            ((RasImplementation)application).setUserIdentification(user, password);
            databaseConnection = application.newDatabase();

            System.out.println("Opening database connection ...");
            databaseConnection.open(database, Database.OPEN_READ_ONLY);

        } catch (org.odmg.ODMGException e) {
            System.out.println("An exception has occurred: " + e.getMessage());
            System.out.println("Aborting ...");
            System.exit(1);
        }
        
        /*
         * Input loop
         */
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = System.in;
            br = new BufferedReader(new InputStreamReader(is));
            String line = null;
            System.out.println("Pose a RaSQL query and confirm with ENTER. Use quit or exit to leave.");
            System.out.println();
            prompt();
            while ((line = br.readLine()) != null) {
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("q")) {
                    break;
                }
                System.out.println("Sending query: " + line);
                query(application, line);
                prompt();
            }
        }
        catch (IOException ioe) {
            System.out.println("Exception while reading input " + ioe);
        }
        finally {
            try {
                if (br != null) {
                    br.close();
                }
            }
            catch (IOException ioe) {
                System.out.println("Error while closing stream: " + ioe);
            } 
        }
        
        /*
         * Closing connection
         */
        try {
            System.out.println("Closing database connection ...");
            databaseConnection.close();
        } catch (org.odmg.ODMGException exp) {
            System.err.println("Could not close the database: "
                    + exp.getMessage());
        }
        
        System.out.println("Goodbye.");
    }

    /**
     * Prints usage information to the console.
     */
    private static void printUsage() {
        System.out.println("Usage:  java -jar jRasql [-help] [-server] [-port] [-database] [-user] [-password]");
        System.out.println("-help:      print this message");
        System.out.println("-server:    network adress to rasmgr (default: localhost)");
        System.out.println("-port:      port where rasmgr is running (default: 7001)");
        System.out.println("-database:  database to connect to (default: RASBASE)");
        System.out.println("-user:      rasmgr user (default: rasguest)");
        System.out.println("-password:  the user's password (default: rasguest)");
        System.out.println();
    }
    
    /**
     * Prints the prompt.
     */
    private static void prompt() {
        System.out.println("RaSQL> ");
    }
            
    /**
     * Opens a transaction with the specified {@link application} and sends the specified {@link queryString}.
     * @param application
     * @param queryString 
     */
    private static void query(Implementation application, String queryString) {
        DBag resultBag = null;
        RasGMArray result = null;
        Transaction transaction = null;
        OQLQuery query = null;
        
        try {
            System.out.println("Opening transaction ...");
            transaction = application.newTransaction();
            transaction.begin();

            System.out.println("Retrieving MDDs ...");
            query = application.newOQLQuery();
            query.create(queryString);
            resultBag = (DBag)query.execute();
            if (resultBag != null) {
                Iterator iter = resultBag.iterator();
                while (iter.hasNext()) {
                    result = (RasGMArray)iter.next();
                    System.out.println(result);
                }
                System.out.println("All results");
            }
            transaction.commit();
            System.out.println("Transaction committed.");
        } catch (org.odmg.ODMGException e) {
            System.out.println("An exception has occurred: " + e.getMessage());
            System.out.println("Try to abort the transaction ...");
            if (transaction != null) {
                transaction.abort();
            }
        }
    }
}

