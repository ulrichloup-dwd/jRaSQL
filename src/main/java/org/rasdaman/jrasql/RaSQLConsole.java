/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rasdaman.jrasql;


import rasj.RasImplementation;
import rasj.RasGMArray;
import org.odmg.Database;
import org.odmg.DBag;
import org.odmg.Implementation;
import org.odmg.Transaction;
import org.odmg.OQLQuery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

//import java.awt.event.KeyAdapter;
//import java.awt.event.KeyEvent;

import java.util.Properties;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * A simple console application for sending Rasdaman Query Language queries. 
 * 
 * @author Ulrich Loup <ulrich.loup@dwd.de>
 */
public class RaSQLConsole { //extends KeyAdapter {

    // constants
    private static final String SERVER = "localhost";
    private static final String PORT = "7001";
    private static final String DATABASE = "RASBASE";
    private static final String USER = "rasquest";
    private static final String PASSWORD = "rasquest";

    // attributes
    protected ArrayList<String> history;
    private int historyIndex;
    
    /**
     * Constructs a RaSQLConsole object.
     */
    private RaSQLConsole() {
        this.history = new ArrayList<String>();
        this.historyIndex = -1;
    }

    
    public static void main(String[] args) {
        String server = SERVER;
        String port = PORT;
        String database = DATABASE;
        String user = USER;
        String password = PASSWORD;

        System.out.println("Rasdaman Query Language (RaSQL) Console");
        RaSQLConsole rasql = new RaSQLConsole(); // constructs singleton
        Properties properties = new Properties();
        try {
            properties.load(rasql.getClass().getClassLoader().getResourceAsStream("project.properties"));
            System.out.println("Version: " + properties.getProperty("version"));
        } catch (IOException ioe) { }
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
            if (!server.startsWith("http://") && !server.startsWith("https://")) server = "http://" + server;
            if (server.endsWith("/")) server = server.substring(0, server.length()-1);
            System.out.println("Connecting to " + server + ":" + port + "...");
            application = new RasImplementation(server + ":" + port);
            ((RasImplementation)application).setUserIdentification(user, password);
            databaseConnection = application.newDatabase();
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
            System.out.println("Pose a RaSQL query and confirm with ENTER. Use 'q', 'quit' or 'exit' to leave and 'h' or 'history' to show the last queries.");
            System.out.println();
            prompt();
            while ((line = br.readLine()) != null) {
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("q")) {
                    break;
                }
                if (line.equalsIgnoreCase("history") || line.equalsIgnoreCase("h")) {
                        rasql.printHistory();
                        prompt();
                        continue;
                }
                line = line.trim();
                if (!line.isEmpty()) {
                    System.out.println("Sending query: " + line);
                    rasql.history.add(line);
                    query(application, line);
                }
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

//    // Methods from KeyAdapter
//    
//    public void keyTyped(KeyEvent e) {
//        if (this.history.isEmpty()) return;
//        if (e.getKeyCode() == KeyEvent.VK_UP) this.historyIndex++;
//        else if (e.getKeyCode() == KeyEvent.VK_DOWN) this.historyIndex--;
//        this.historyIndex = this.historyIndex % this.history.size();
//        System.out.print('\u000C'); // should clear the console
//        prompt();
//        System.out.print(this.history.get(this.historyIndex));
//    }
    
    /**
     * Prints the history of all queries.
     */
    protected void printHistory() {
        Iterator iter = this.history.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
    }
    
    // Static Methods
    
    /**
     * Prints the prompt.
     */
    private static void prompt() {
        System.out.print("RaSQL> ");
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
                if( resultBag.size() == 1 )
                    System.out.println("Query result: " + resultBag);
                else {
                    System.out.println("Query result count: " + resultBag.size());
                    Iterator iter = resultBag.iterator();
                    while (iter.hasNext()) {
                        result = (RasGMArray)iter.next();
                        // result.getTypeSchema()
                        // System.out.println(result.getObjectTypeName());
                        System.out.println(result);
                    }
                    System.out.println("All results");
                }
            }
            transaction.commit();
            System.out.println("Transaction committed.");
        } catch (org.odmg.ODMGException e) {
            System.out.println("An exception has occurred: " + e.getMessage());
            if (transaction != null) {
                transaction.abort();
                System.out.println("Transaction aborted.");
            }
        }
    }
}

