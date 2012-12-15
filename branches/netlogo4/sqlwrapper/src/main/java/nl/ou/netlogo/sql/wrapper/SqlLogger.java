/*
 * Copyright 2010, 2011 Open University of The Netherlands
 * Contributors: Jan Blom, Rene Quakkelaar, Mark Rotteveel
 *
 * This file is part of NetLogo SQL Wrapper extension.
 * 
 * NetLogo SQL Wrapper extension is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License 
 * as published by the Free Software Foundation, either version 3 of the 
 * License, or (at your option) any later version.
 * 
 * NetLogo SQL Wrapper is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with NetLogo SQL Wrapper extension.  If not, 
 * see <http://www.gnu.org/licenses/>.
 */
package nl.ou.netlogo.sql.wrapper;

import java.util.logging.*;
import org.nlogo.api.*;

/**
 * Configurable wrapper class for the standard Java Logger class
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlLogger implements SqlConfigurable {

    private static final String loggerName = "nl.ou.netlogo";
    private static MyLogger logger = null;
    private static FileHandler fileHandler;

    /*
     * internal logger to intercept all logging
     */
    private static class MyLogger extends Logger {
        private Logger realLogger = null;
        private boolean copy2StdErr = false;

        public MyLogger(String name) {
            super(name, null);
            if (realLogger == null) {
                realLogger = Logger.getLogger(name);
                initializeLogger();
            }
            this.setLevel(Level.ALL);
        }

        public void log(LogRecord rec) {
            if (copy2StdErr) {
                System.err.println("LOGGED (" + rec.getLevel() + "): " + rec.getMessage());
            }
            realLogger.log(rec);
        }

        private void initializeLogger() {
            /*
             * Create the logger. By default, set up a memory handler that will
             * only push SEVERE messages to the console, (buffer size 1 makes it
             * discard any other messages), and switch all parent handlers off
             */
            realLogger.setUseParentHandlers(false);

        }

        public void setCopy2StdErr(boolean toggle) {
            copy2StdErr = toggle;
        }

    }
    
    public SqlLogger() {
    	synchronized(SqlLogger.class) {
            if (logger == null) {
                logger = new MyLogger(loggerName);
            }
    	}
    }

    public static Logger getLogger() {
        if (logger == null) {
            // if our own logger wasn't initialized, give out the default one
            return Logger.getLogger(loggerName);
        }
        return logger;
    }

    /**
     * Interface to logger for sql:log command
     * 
     * @param level
     * @param message
     */
    public static void sqlLog(String level, String message, Context context) {
        Level logLevel = interpretLevel(level);
        if (logLevel == Level.ALL) {
            // unknown level, log as INFO and prefix the unknown level to the message
            logLevel = Level.INFO;
            message = "(" + level.toUpperCase() + ") " + message;
        }
        logger.log(logLevel, message + "(from: " + context + ")");
    }

    /*
     * (non-Javadoc)
     * 
     * @see {@link
     * nl.ou.netlogo.sql.wrapper.SqlConfigurable#configure(nl.ou.netlogo
     * .sql.wrapper.SqlSetting, org.nlogo.api.Context)}
     */
    public void configure(SqlSetting settings, Context context) throws ExtensionException {
        Logger LOG = SqlLogger.getLogger();
        LOG.finest("SqlLogger.configure()");
        if (settings.getName().equals(SqlConfiguration.LOGGING)) {
            if (!settings.isValid()) {
                return;
            }
            try {
                LOG.finest("Starting to configure the logger");
                boolean toggle = SqlSetting.toggleValue(settings.getString(SqlConfiguration.LOGGING_OPT_LOGGING));
                if (toggle) {
                    synchronized(SqlLogger.class) {
                        if (fileHandler == null) {
                            /*
                             * create a file handler
                             */
                            try {
                                fileHandler = new FileHandler(parseLogPath(
                                        settings.getString(SqlConfiguration.LOGGING_OPT_PATH), context)
                                        + "/sqlwrapper.log");
                                fileHandler.setFormatter(new SimpleFormatter());
                                fileHandler.setLevel(Level.ALL);
    
                                // add a shutdown hook to close down the file handler at program exit
                                Runtime.getRuntime().addShutdownHook(new Thread() {
                                    public void run() {
                                        if (fileHandler != null) {
                                            fileHandler.close();
                                        }
                                    }
                                });
                            } catch (Exception ex) {
                                String message = "Cannot configure logging file handler: " + ex;
                                LOG.severe(message);
                                throw new ExtensionException(message);
                            }
                        }
                    }
                }
                LOG.finest("Handler done, now doing logger settings");
                logger.setCopy2StdErr(SqlSetting.toggleValue(settings
                        .getString(SqlConfiguration.LOGGING_OPT_COPYTOSTDERR)));
                setLevel(logger, settings.getString(SqlConfiguration.LOGGING_OPT_LEVEL));

                LOG.fine("Logging configured");
                LOG.fine("    path: " + parseLogPath(settings.getString(SqlConfiguration.LOGGING_OPT_PATH), context));
                LOG.fine("    file handler: " + settings.getString(SqlConfiguration.LOGGING_OPT_LOGGING));
                LOG.fine("    copy to stderr: " + settings.getString(SqlConfiguration.LOGGING_OPT_COPYTOSTDERR));
            } catch (Exception ex) {
                String message = "Unexpected exception: " + ex;
                LOG.severe(message);
                ex.printStackTrace();
                throw new ExtensionException(ex);
            }
        }
    }

    /**
     * Configures the path for the log files
     * 
     * @param settings
     * @param context
     * @throws Exception
     */
    private String parseLogPath(String path, Context context) throws Exception {
        if (context != null) {
            /*
             * special treatment: if path starts with "%m" or "%c", it is
             * replaced by the model directory or the current directory of the
             * context, respectively
             */
            if (path.startsWith("%m")) {
                path = context.attachModelDir(".") + path.substring(2);
            } else if (path.startsWith("%c")) {
                path = context.attachCurrentDirectory(".") + path.substring(2);
            }
        } else {
            // without a valid context, remove %m or %c at start of path
            if (path.startsWith("%m") || path.startsWith("%c")) {
                path = path.substring(2);
            }
        }
        return path;
    }

    /**
     * Sets the minimum log level for the given handler
     * 
     * @param handler
     * @param level
     */
    private static void setLevel(Logger logger, String level) {
        if (logger != null) {
            Level logLevel = interpretLevel(level);
            if (logLevel != Level.ALL) {
                logger.setLevel(logLevel);
            }
        }
    }

    /**
     * Interprets a string as a Level value. Returns Level.ALL if the string
     * could not be interpreted.
     * 
     * @param level
     * @return LEVEL value
     */
    private static Level interpretLevel(String level) {
        Level logLevel = Level.ALL;
        if (level.equalsIgnoreCase("severe")) {
            logLevel = Level.SEVERE;
        } else if (level.equalsIgnoreCase("warning")) {
            logLevel = Level.WARNING;
        } else if (level.equalsIgnoreCase("info")) {
            logLevel = Level.INFO;
        } else if (level.equalsIgnoreCase("fine")) {
            logLevel = Level.FINE;
        } else if (level.equalsIgnoreCase("finer")) {
            logLevel = Level.FINER;
        } else if (level.equalsIgnoreCase("finest")) {
            logLevel = Level.FINEST;
        } else if (level.equalsIgnoreCase("off")) {
            logLevel = Level.OFF;
        }
        return logLevel;
    }
}
