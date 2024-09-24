package common;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Utility class that redirects System.out and System.err to a formatted print stream.
 * Redirects System.out and System.err to a formatted print stream that includes the current date, time, and thread name.
 */
public class FormattedSystemOut {

    /**
     * Sets formatted sys out.
     *
     * @param logFilePath the log file path
     * @throws FileNotFoundException the file not found exception
     */
    public static void setupFormattedSysOut(String logFilePath) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(logFilePath, false);
        PrintStream formattedPrintStream = new PrintStream(fos) {
            private final SimpleDateFormat dateFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");

            @Override
            public void println(String x) {
                // Synchronized formatting and output
                synchronized (this) {
                    super.println(formatMessage(x));
                }
            }

            @Override
            public void println(Object x) {
                // Synchronized formatting and output for any object
                synchronized (this) {
                    super.println(formatMessage(String.valueOf(x)));
                }
            }

            private String formatMessage(String message) {
                // Include thread name in the message
                return dateFormat.format(new Date()) + " [" + Thread.currentThread().getName() + "] " + message;
            }
        };

        // Redirect System.out and System.err to the formatted print stream
        System.setOut(formattedPrintStream);
        System.setErr(formattedPrintStream);
    }
}