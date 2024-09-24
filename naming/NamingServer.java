package naming;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import common.BooleanReturn;
import common.CopyRequest;
import common.ExceptionReturn;
import common.FilesReturn;
import common.LockRequest;
import common.Path;
import common.PathRequest;
import common.RegisterRequest;
import common.ServerInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static common.FormattedSystemOut.setupFormattedSysOut;

/**
 * The NamingServer class is responsible for managing the file system structure and the storage servers that store the files.
 */
public class NamingServer {
    /** Interval for replicating files on read. */
    private static final int REPLICATION_READ_INTERVAL = 20;
    /** Gson object for JSON serialization and deserialization. */
    private final Gson gson = new Gson();
    /** Map of stored paths to their directory status. */
    private final Map<Path, Boolean> storedPaths;
    /** Map of paths to the storage servers that store the files. */
    private final Map<Path, List<ServerInfo>> pathToServerInfo = new ConcurrentHashMap<>();
    /** Set of registered storage servers. */
    private final Set<String> registeredStorageServers = ConcurrentHashMap.newKeySet();
    /** The port for the naming server. */
    private final int servicePort;
    /** The port for the storage server to register server. */
    private final int registrationPort;
    /** Map of paths to their locks. */
    private final Map<Path, ReadWriteLock> lockMap;


    /**
     * Checks if a path exists or is a subpath of a stored path.
     * @param path the path to check
     * @return true if the path exists or is a subpath of a stored path, false otherwise
     */
    private boolean pathExists(Path path) {
        return storedPaths.keySet().stream()
                .anyMatch(storedPath -> storedPath.equals(path) || path.isSubpath(storedPath));
    }

    /**
     * Checks if a path is a directory.
     * @param pathToCheck the path to check
     * @return true if the path is a directory, false otherwise
     */
    private boolean isDirectory(Path pathToCheck) {
        try {
            return storedPaths.keySet()
                    .stream()
                    .anyMatch(storedPath -> !storedPath.isRoot() && storedPath.parent().equals(pathToCheck));
        } catch (Exception e) {
            System.out.println("Error checking if path is a directory: " + e.getMessage());
            return false;
        }
    }

    /**
     * Returns the storage server information for a given path.
     * @param path the path to get the server info for
     * @return the server info for the path
     */
    public ServerInfo getServerInfoForPath(Path path) {
        return pathToServerInfo.get(path).get(0);
    }

    /**
     * The constructor for the NamingServer class.
     *
     * @param servicePort The port for the naming server.
     * @param registrationPort The port for the storage server to register server.
     */
    public NamingServer(int servicePort, int registrationPort) {
        // Setting up formatted sys out for logging in the file naming_server.log
        try {
            setupFormattedSysOut("naming_server.log");
        } catch (FileNotFoundException e) {
            System.out.println("Error creating log file: " + e.getMessage());
        }
        this.storedPaths = new ConcurrentHashMap<>();
        this.lockMap = new ConcurrentHashMap<>();
        this.servicePort = servicePort;
        this.registrationPort = registrationPort;
        storedPaths.put(new Path("/"), true);
    }


    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: java NamingServer <service-port> <registration-port>");
            return;
        }
        int servicePort = Integer.parseInt(args[0]);
        int registrationPort = Integer.parseInt(args[1]);
        NamingServer namingServer = new NamingServer(servicePort, registrationPort);
        namingServer.startNamingServer();
    }

    /**
     * Starts the naming server.
     * This method creates the HTTP server for the naming server and registers the endpoints for the naming server services.
     * The naming server provides the following services:
     * - Path validation /is_valid_path
     * - List directory /list
     * - Check if a path is a directory /is_directory
     * - Create file /create_file
     * - Create directory /create_directory
     * - Get storage server for a path /get_storage
     * - Lock a path /lock
     * - Unlock a path /unlock
     * - Delete a file or directory /delete
     * - Register a storage server /register
     * The naming server listens on two ports: one for the naming server services and one for the storage server registration.
     */
    private void startNamingServer() {
        try {
            HttpServer serviceServer = HttpServer.create(new InetSocketAddress(servicePort), 0);
            serviceServer.createContext("/is_valid_path", this::handlePathValidation);
            serviceServer.createContext("/list", this::handleList);
            serviceServer.createContext("/is_directory", this::handleIsDirectory);
            serviceServer.createContext("/create_file", exchange -> this.handleCreate(exchange, false));
            serviceServer.createContext("/create_directory", exchange -> this.handleCreate(exchange, true));
            serviceServer.createContext("/get_storage", this::handleGetStorage);
            serviceServer.createContext("/lock", this::handleLock);
            serviceServer.createContext("/unlock", this::handleUnlock);
            serviceServer.createContext("/delete", this::handleDelete);
            ExecutorService executorService = Executors.newFixedThreadPool(5000);
            serviceServer.setExecutor(executorService);
            serviceServer.start();

            HttpServer registrationServer = HttpServer.create(new InetSocketAddress(registrationPort), 0);
            registrationServer.createContext("/register", this::handleRegistration);
            registrationServer.setExecutor(null); // creates a default executor
            registrationServer.start();

            System.out.println("Naming server started on ports " + servicePort + " and " + registrationPort);
        } catch (Exception e) {
            System.out.println("Error starting the naming server: " + e.getMessage());
        }
    }

    /**
     * Sends an HTTP request to a storage server.
     * @param exchange the HTTP exchange
     * @throws IOException if an I/O error occurs
     */
    private void handleDelete(HttpExchange exchange) throws IOException {
        System.out.println("---------------- Received delete request ----------------");
        InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
        PathRequest pathRequest = gson.fromJson(isr, PathRequest.class);
        ExceptionReturn exceptionReturn;

        // If the path is null or empty, respond with IllegalArgumentException.
        if (pathRequest == null || pathRequest.path == null || pathRequest.path.isEmpty()) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            System.out.println("got null path, sent bad request response for path: "+ pathRequest.path);
            return;
        }

        // Attempt to delete the file/directory.
        try {
            boolean success = deleteFileDir(pathRequest.path);
            if (success) {
                // If deletion was successful, respond with true.
                sendResponse(exchange, HttpURLConnection.HTTP_OK, new BooleanReturn(true));
            } else {
                // If the file/directory could not be deleted for any other reason, treat as an internal error.
                System.out.println("Failed to delete due to an internal error: " + pathRequest.path);
                sendResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, new ExceptionReturn("InternalError", "Failed to delete the requested resource."));
            }
        } catch (FileNotFoundException e) {
            // Correctly handle FileNotFoundException with appropriate response.
            System.out.println("File/Directory not found for deletion: " + pathRequest.path);
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "The file/directory or parent directory does not exist."));
        } catch (IllegalArgumentException e) {
            // Handle any other IllegalArgumentException appropriately.
            System.out.println("Invalid path for deletion: " + pathRequest.path);
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", e.getMessage()));
        }
    }

    /**
     * Delete a file or a directory.
     * @param path the path to the file
     * @return true if the file is deleted, false otherwise
     * @throws FileNotFoundException if the file does not exist
     * @throws IllegalArgumentException if the path is invalid or any other error occurs
     */
    private boolean deleteFileDir(String path) throws FileNotFoundException, IllegalArgumentException {
        if (!isValidPath(path)) {
            throw new IllegalArgumentException("Invalid directory path.");
        }

        Path filePath = new Path(path);
        if (!pathExists(filePath)) {
            throw new FileNotFoundException("The file/directory does not exist: " + path);
        }

        try {
            for (String storageServer : registeredStorageServers) {
                // Remove the file or directory from the storage server
                removeFileOnStorage(storageServer, path);
            }

            // Remove the path from storedPaths
            storedPaths.remove(new Path(path));

            // Remove the path from pathToServerInfo
            pathToServerInfo.remove(new Path(path));

            return true;
        } catch (IOException | InterruptedException e) {
            throw new FileNotFoundException("Failed to delete file/directory: " + path);
        }
    }

    /**
     * Sends an HTTP request to a storage server.
     * @param path the path to the file
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted
     */
    private void removeFileOnStorage(String storageServer, String path) throws IOException, InterruptedException {
        // Extract the storage server's command port from the storageServer string
        String[] serverParts = storageServer.split(":");
        int commandPort = Integer.parseInt(serverParts[2]);

        // Send a delete request to the storage server
        HttpResponse<String> response = sendCommand("/storage_delete", commandPort, new PathRequest(path));

        // Check the response status code
        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            // If the deletion failed, throw an exception
            ExceptionReturn exceptionReturn = gson.fromJson(response.body(), ExceptionReturn.class);
            throw new IOException("Error deleting file on storage server: " + exceptionReturn.exception_info);
        }
    }

    /**
     * Handles the create request for a file or directory as per the REST API documentation.
     * @param exchange the HTTP exchange
     * @param isDirectory true if received a create_directory request, false if received a create_file request
     * @throws IOException if an I/O error occurs
     */
    private void handleCreate(HttpExchange exchange, boolean isDirectory) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            return;
        }

        PathRequest request;
        try (InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)) {
            request = gson.fromJson(reader, PathRequest.class);
        } catch (Exception e) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("JsonParseException", "Invalid request format."));
            return;
        }

        if (request == null || request.path == null || request.path.isEmpty()) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }
        System.out.println("create request received : " + request.path);
        if (request.path == null || request.path.isEmpty()) {
            try {
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            } catch (IOException e) {
                System.out.println("Error sending response headers: " + e.getMessage());
            }
            return;
        }
        if(!isValidPath(request.path)) {
            try {
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            } catch (IOException e) {
                System.out.println("Error sending response headers: " + e.getMessage());
            }
            return;
        }
        Path path = new Path(request.path);
        if (pathExists(path)) {
            try {
                System.out.println("Path already exists: " + path);
                sendResponse(exchange, HttpURLConnection.HTTP_OK, new BooleanReturn(false));
            } catch (IOException e) {
                System.out.println("Error sending response headers: " + e.getMessage());
            }
            return;
        }
        Path parentDirectory = path.parent();
        if(!pathExists(parentDirectory)) {
            try {
                System.out.println("Parent directory does not exist: " + parentDirectory);
                sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "the parent directory does not exist."));
            } catch (IOException e) {
                System.out.println("Error sending response headers: " + e.getMessage());
            }
            return;
        }
        if(storedPaths.get(parentDirectory) != null && !storedPaths.get(parentDirectory)) {
            try {
                System.out.println("Parent directory is not a directory: " + parentDirectory);
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("FileNotFoundException", "the parent path is not a directory."));
            } catch (IOException e) {
                System.out.println("Error sending response headers: " + e.getMessage());
            }
            return;
        }
        storedPaths.put(path, isDirectory);
        if(!isDirectory) {
            System.out.println("Creating file on storage server: " + path);
            createFileOnStorageServer(exchange, path);
        }
        try {
            sendResponse(exchange, HttpURLConnection.HTTP_OK, new BooleanReturn(true));
        } catch (IOException e) {
            System.out.println("Error sending response headers: " + e.getMessage());
        }
    }

    /**
     * Handles the path validation request as per the REST API documentation.
     * @param exchange the HTTP exchange
     * @throws IOException if an I/O error occurs
     */
    private void handlePathValidation(HttpExchange exchange) throws IOException {
        try {
            if ("POST".equals(exchange.getRequestMethod())) {
                InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), "UTF-8");
                PathRequest pathRequest = gson.fromJson(reader, PathRequest.class);
                boolean isValid = isValidPath(pathRequest.path);

                String response = gson.toJson(new BooleanReturn(isValid));
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        } catch (Exception e) {
            System.out.println("Error validating path: " + e.getMessage());
        }
    }

    /**
     * Checks if a path is valid.
     * @param path the path to check
     * @return true if the path is valid, false otherwise
     */
    private boolean isValidPath(String path) {
        return path != null && path.startsWith("/") && !path.contains(":") && !path.contains(" ");
    }


    /**
     * Handles the registration request for a storage server as per the REST API documentation.
     * @param exchange the HTTP exchange
     * @throws IOException if an I/O error occurs
     */
    private void handleRegistration(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1); // Method Not Allowed
            return;
        }

        // Extract the RegisterRequest from the request body
        InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), "UTF-8");
        RegisterRequest request = gson.fromJson(reader, RegisterRequest.class);
        System.out.println("register request received : " + request);
        // Validate the request (this is simplistic validation)
        if (request.storage_ip == null || request.files == null) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return;
        }

        // Concatenate IP and port to check duplicates
        String storageServerKey = request.storage_ip + ":" + request.client_port + ":" + request.command_port;

        System.out.println("registering:" + storageServerKey);


        // Here, add the storage server to the naming server's records

        boolean registrationSuccess = registerStorageServer(storageServerKey);

        // Decide on the response based on registration success
        if (registrationSuccess) {
            // Respond with success and optionally any instructions for the storage server
            String[] deletedFiles = addFiles(request.files, storageServerKey);
            FilesReturn filesReturn = new FilesReturn(deletedFiles);
            String response = gson.toJson(filesReturn);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        } else {
            System.out.println("register fail");
            // Respond with an exception indicating registration failure
            ExceptionReturn exceptionReturn = new ExceptionReturn("IllegalStateException", "This storage server is already registered.");
            String response = gson.toJson(exceptionReturn);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CONFLICT, response.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    /**
     * Add files to the stored paths and path to server info maps.
     * Also, identify and add files to be deleted on the storage server
     * @param files the files array from the storage server to add
     * @param storageServerKey the storage server key
     * @return the files to be deleted
     */
    private String[] addFiles(String[] files, String storageServerKey) {
        List<String> deletedFiles = new ArrayList<>();

        for (String file : files) {
            if(file.equals("/")) {
                return deletedFiles.toArray(new String[0]);
            }
            Path filePath = new Path(file);
            // Check if the path is already stored or is a subpath of any stored path
            for (Path storedPath : storedPaths.keySet()) {
                System.out.println("comparing stored path: " + storedPath + " with file path: " + filePath);
                if (storedPath.equals(filePath) || filePath.isSubpath(storedPath)) {
                    // Add the path to deleted files
                    deletedFiles.add(file);
                    break; // Break the loop if the file is added to deleted files
                }
            }
            // If the file was not added to deleted files, add it to the stored paths set
            if (!deletedFiles.contains(file)) {
                System.out.println("adding file to stored paths: " + filePath);
                storedPaths.put(filePath, false);
                String[] serverParts = storageServerKey.split(":");
                pathToServerInfo.put(filePath, List.of(new ServerInfo(serverParts[0], Integer.parseInt(serverParts[1]))));
                System.out.println("Server info added for path: " + filePath + " - " + pathToServerInfo.get(filePath));
            }
        }

        // Convert the list of deleted files to an array
        return deletedFiles.toArray(new String[0]);
    }

    /**
     * Register a storage server.
     * Update the registered storage servers set and perform any additional registration logic.
     * @param storageServerKey the storage server key
     * @return true if the storage server is successfully registered, false otherwise
     */
    private boolean registerStorageServer(String storageServerKey) {
        // Check if the storage server is already registered
        if (registeredStorageServers.contains(storageServerKey)) {
            // Storage server is already registered, return false to indicate failure
            return false;
        }
        // Add the new storage server to the registered servers set
        registeredStorageServers.add(storageServerKey);
        return true;
    }


    /**
     * Handles the HTTP request for the /is_directory endpoint as per the REST API documentation.
     * @param exchange the HTTP exchange
     * @throws IOException if an I/O error occurs
     */
    private void handleIsDirectory(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            return;
        }

        PathRequest request;
        try (InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)) {
            request = gson.fromJson(reader, PathRequest.class);
        } catch (Exception e) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("JsonParseException", "Invalid request format."));
            return;
        }

        if (request == null || request.path == null || request.path.isEmpty()) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }
        System.out.println("is_directory request received : " + request.path);
        Path requestPath = new Path(request.path);
        if (requestPath.isRoot()) {
            sendResponse(exchange, HttpURLConnection.HTTP_OK, new BooleanReturn(true));
            return;
        }

        if (!pathExists(requestPath)) {
            System.out.println("Requested Path does not exist: " + requestPath);
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "the file/directory or parent directory does not exist."));
            return;
        }
        boolean isDirectory = storedPaths.getOrDefault(requestPath, false) || isDirectory(requestPath);
        sendResponse(exchange, HttpURLConnection.HTTP_OK, new BooleanReturn(isDirectory));
    }


    /**
     * Handles the HTTP request for the /list endpoint as per the REST API documentation.
     * @param exchange the HTTP exchange
     * @throws IOException if an I/O error occurs
     */
    private void handleList(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, new ExceptionReturn("WrongMethod", "Only support POST method."));
            return;
        }

        PathRequest request;
        try (InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), "UTF-8")) {
            request = gson.fromJson(reader, PathRequest.class);
        } catch (Exception e) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("JsonParseException", "Invalid request format."));
            return;
        }

        if (request == null || request.path == null || request.path.isEmpty()) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }

        Path requestPath = new Path(request.path);
        System.out.println("list path request received : " + requestPath);

        if (requestPath.isRoot()) {
            processRootDirectory(exchange, request.path);
            return;
        }

        if (!pathExists(requestPath)) {
            System.out.println("Path does not exist returning response: " + requestPath);
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "Path does not exist."));
            return;
        }

        if (isDirectory(requestPath)) {
            processDirectoryListing(exchange, requestPath);
        } else {
            System.out.println("Path is not a directory: " + requestPath);
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("FileNotFoundException", "Path is not a directory."));
        }
    }

    /**
     * Processes the root directory and sends a response with the names of its direct children.
     * This method is used to handle HTTP requests for listing the contents of the root directory.
     *
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @param requestPath The path of the root directory to process.
     * @throws IOException If an I/O error occurs.
     */
    private void processRootDirectory(HttpExchange exchange, String requestPath) throws IOException {
        try {
            String dirPath = requestPath;
            if (!dirPath.endsWith("/")) {
                dirPath += "/";
            }
            String finalDirPath = dirPath;
            String[] children = storedPaths.keySet().stream()
                    .map(Path::toString) // Convert Path objects to their String representation.
                    .filter(path -> path.startsWith(finalDirPath)) // Filter paths that start with the directory path.
                    .map(path -> path.substring(finalDirPath.length())) // Remove the directory part from the path.
                    .filter(subpath -> !subpath.isEmpty()) // Exclude the directory itself if listed.
                    .map(subpath -> subpath.contains("/") ? subpath.substring(0, subpath.indexOf("/")) : subpath) // Extract the direct child name.
                    .distinct() // Ensure uniqueness.
                    .toArray(String[]::new); // Collect to array
            sendResponse(exchange, HttpURLConnection.HTTP_OK, new FilesReturn(children));
        } catch (Exception e) {
            System.out.println("Error processing root directory: " + e.getMessage());
        }
    }

    /**
     * Processes a directory listing and sends a response with the names of its direct children.
     * This method is used to handle HTTP requests for listing the contents of a directory.
     *
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @param directoryPath The path of the directory to process.
     * @throws IOException If an I/O error occurs.
     */
    private void processDirectoryListing(HttpExchange exchange, Path directoryPath) throws IOException {
        System.out.println("Listing directory: " + directoryPath + " in stored paths " + storedPaths);
        try {
            String[] children = storedPaths.keySet().stream()
                    .filter(path -> {
                        // Check if the path's parent matches the directory being listed.
                        // Also, ensure we're not including the directory itself in its listing.
                        return !path.isRoot() && path.parent().equals(directoryPath) && !path.equals(directoryPath);
                    })
                    .map(Path::last) // Extract the last component of each path.
                    .distinct() // Ensure uniqueness in case of overlapping names.
                    .toArray(String[]::new); // Convert to array.
            sendResponse(exchange, HttpURLConnection.HTTP_OK, new FilesReturn(children));
        } catch (Exception e) {
            System.out.println("Error processing directory listing: " + e.getMessage());
        }
    }


    /**
     * Handles the HTTP request for the /get_storage endpoint as per the REST API documentation.
     * This method is responsible for retrieving the storage server information for a given path.
     *
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @throws IOException If an I/O error occurs.
     */
    private void handleGetStorage(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            return;
        }

        PathRequest request;
        try (InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), "UTF-8")) {
            request = gson.fromJson(reader, PathRequest.class);
        } catch (Exception e) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("JsonParseException", "Invalid request format."));
            return;
        }

        if (request == null || request.path == null || request.path.isEmpty() || !isValidPath(request.path)) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }
        System.out.println("get_storage request received : " + request.path);
        Path path = new Path(request.path);
        if (!pathExists(path)) {
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "Path does not exist."));
            return;
        }

        if (isDirectory(path)) {
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("FileNotFoundException", "Path is a directory."));
            return;
        }
        ServerInfo info = this.getServerInfoForPath(path);
        if (info != null) {
            sendResponse(exchange, HttpURLConnection.HTTP_OK, info);
        } else {
            sendResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, new ExceptionReturn("ServerError", "Unable to retrieve server info."));
        }
    }


    /**
     * Handles the HTTP request for the /lock endpoint as per the REST API documentation.
     * This method is responsible for locking a given path.
     *
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @throws IOException If an I/O error occurs.
     */
    private void handleLock(HttpExchange exchange) throws IOException {
        System.out.println("========== Received lock request. ==========");

        InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
        Gson gson = new Gson();
        LockRequest request = gson.fromJson(isr, LockRequest.class);
        System.out.println("Lock request for path: " + request.path + ", Exclusive: " + request.exclusive);

        if (request.path == null || request.path.trim().isEmpty()) {
            System.out.println("Lock request error: Path is null or empty.");
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }

        Path path = new Path(request.path);

        if (!pathExists(path)) {
            System.out.println("Lock request error: Path does not exist.");
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", "The file/directory cannot be found."));
            return;
        }

        try {
            lockFiles(path, request.exclusive);
            System.out.println("Path locked successfully.");
            sendResponse(exchange, HttpURLConnection.HTTP_OK, null);
        } catch (FileNotFoundException e) {
            System.out.println("Lock request error: " + e.getMessage());
            sendResponse(exchange, HttpURLConnection.HTTP_NOT_FOUND, new ExceptionReturn("FileNotFoundException", e.getMessage()));
        } catch (Exception e) {
            System.out.println("Lock request error: " + e.getMessage());
            sendResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, new ExceptionReturn("InternalError", "An unexpected error occurred while processing the lock request."));
        }
    }


    /**
     * Handles the HTTP request for the /unlock endpoint as per the REST API documentation.
     * This method is responsible for unlocking a given path.
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @throws IOException If an I/O error occurs.
     */
    private void handleUnlock(HttpExchange exchange) throws IOException {
        System.out.println("============== Received unlock request. =================");

        InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
        Gson gson = new Gson();
        LockRequest request = gson.fromJson(isr, LockRequest.class);
        System.out.println("Unlock request for path: " + request.path + ", Exclusive: " + request.exclusive);

        if (request.path == null || request.path.trim().isEmpty()) {
            System.out.println("Unlock request error: Path is null or empty.");
            sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new ExceptionReturn("IllegalArgumentException", "Path cannot be null or empty."));
            return;
        }


        Path path = new Path(request.path);

        try {
            unlockFiles(path, request.exclusive);
            System.out.println("Path unlocked successfully.");
            sendResponse(exchange, HttpURLConnection.HTTP_OK, null);
        } catch (IllegalArgumentException e) {
            System.out.println("Unlock request error: " + e.getMessage());
            sendResponse(exchange, HttpURLConnection.HTTP_CONFLICT, new ExceptionReturn("IllegalArgumentException", e.getMessage()));
        } catch (Exception e) {
            System.out.println("Unlock request error: " + e.getMessage());
            sendResponse(exchange, HttpURLConnection.HTTP_INTERNAL_ERROR, new ExceptionReturn("InternalError", "An unexpected error occurred while processing the unlock request."));
        }
    }

    /**
     * This method locks the given path.
     * It locks the path and its parent directories with a shared lock.
     * The lock is acquired from top to bottom to avoid deadlocks.
     * @param path The path to lock
     * @param exclusive True if the lock is exclusive, false otherwise
     * @throws IOException If an I/O error occurs
     * @throws InterruptedException If the thread is interrupted
     */
    private void lockFiles(Path path, boolean exclusive) throws IOException, InterruptedException {
        try {
            if(!path.isRoot()) {
                List<Path> pathsToLock = getSubPathsToLockUnlock(path);
                for(Path pathToLock : pathsToLock) {
                    ReadWriteLock rwLock = lockMap.computeIfAbsent(pathToLock, k -> new ReadWriteLock());
                    System.out.println("Locking path: " + pathToLock);
                    rwLock.lock(false);
                    System.out.println("Path locked: " + pathToLock);
                }
            }
            ReadWriteLock rwLock = lockMap.computeIfAbsent(path, k -> new ReadWriteLock());
            rwLock.lock(exclusive);


            if (rwLock.getTotalReaders() != 0 && rwLock.getTotalReaders() % REPLICATION_READ_INTERVAL == 0) {
                System.out.println("Replicating file: " + path);
                replicateFile(path);
            }

            if (exclusive && !rwLock.isWriteOperationInProgress()) {
                rwLock.setWriteOperationInProgress(true);
                invalidateReplicas(path);
            }
        } catch (Exception e) {
            System.out.println("Error locking single file: " + e.getMessage());
            throw e;
        }
    }


    /**
     * Invalidates (deletes) replicas of a file from all but the primary storage server.
     * This method is intended to ensure that only the most recent version of a file is retained
     * in the system, by removing outdated replicas from secondary servers. This operation is
     * crucial for maintaining data consistency across the distributed file system.
     *
     * @param path The {@link Path} object representing the path to the file whose replicas are to be invalidated.
     *             This path should be the full path of the file as recognized by the storage servers.
     * @throws IOException If an I/O error occurs when communicating with the storage servers to delete the replicas.
     * @throws InterruptedException If the thread is interrupted while waiting to send commands to the storage servers.
     *
     * Note: The method assumes that the first server in the list of {@code serverInfos} is the primary server
     *       and therefore retains the file. Replicas on all other servers (from index 1 onwards) are considered
     *       for deletion.
     */

    private void invalidateReplicas(Path path) throws IOException, InterruptedException {
        List<ServerInfo> serverInfos = pathToServerInfo.get(path);
        if (serverInfos == null || serverInfos.isEmpty()) {
            return;
        }

        //Delete from the rest
        for(int i = 1; i < serverInfos.size(); ++i) {
            PathRequest pq = new PathRequest(path.toString());

            int commandPort = 0;

            for(String registerServer : registeredStorageServers) {
                int serverPort = Integer.valueOf(registerServer.split(":")[1]);
                if(serverPort == serverInfos.get(i).server_port) {
                    commandPort = Integer.valueOf(registerServer.split(":")[2]);
                    break;
                }
            }
            sendCommand("/storage_delete", commandPort, pq);
        }
    }

    /**
     * Initiates replication of a specified file across the distributed file system to ensure redundancy and high availability.
     * This method locates an available storage server that does not already host the file and sends a command to copy the file
     * from the primary storage server to the selected target server. The method aims to enhance data durability and accessibility
     * by maintaining multiple copies of the file across different servers.
     * @param path the path to the file
     * @throws IOException If an I/O error occurs
     * @throws InterruptedException If the thread is interrupted
     */
    private void replicateFile(Path path) throws IOException, InterruptedException {
        try {
            if (!pathToServerInfo.containsKey(path)) {
                return;
            }

            Set<Integer> portsByPath = new HashSet<>();
            for (ServerInfo serverInfo : pathToServerInfo.get(path)) {
                portsByPath.add(serverInfo.server_port);
            }

            int serverClientPort = 0;
            int serverCommandPort = 0;

            // Convert the Set to an array to access a random server
            String[] serversArray = registeredStorageServers.toArray(new String[0]);
            while (true) {
                int randomIndex = ThreadLocalRandom.current().nextInt(serversArray.length);
                String selectedServer = serversArray[randomIndex];
                Integer clientPort = Integer.valueOf(selectedServer.split(":")[1]);
                if (!portsByPath.contains(clientPort)) {
                    serverClientPort = Integer.valueOf(selectedServer.split(":")[1]);
                    serverCommandPort = Integer.valueOf(selectedServer.split(":")[2]);
                    break;
                }
            }

            String targetIp = pathToServerInfo.get(path).get(0).server_ip;
            int targetPort = pathToServerInfo.get(path).get(0).server_port;

            // Construct the copy request
            CopyRequest copyRequest = new CopyRequest(path.toString(), targetIp, targetPort);
            // Attempt to send the command to the replication service

            sendCommand("/storage_copy", serverCommandPort, copyRequest);

            List<ServerInfo> newInfos = new ArrayList<>();

            List<ServerInfo> prevInfos = pathToServerInfo.get(path);

            if (prevInfos == null) {
                // The path does not exist in the map, so create a new list
                prevInfos = new ArrayList<>();
                pathToServerInfo.put(path, prevInfos); // Associate the new list with the path
            }

            ServerInfo newServer = new ServerInfo(targetIp, serverClientPort);

            newInfos.add(newServer);

            for(ServerInfo info : prevInfos) {
                newInfos.add(info);
            }
            pathToServerInfo.put(path, newInfos);
            // Optionally, track the replication status or perform additional operations here
        } catch (IOException e) {
            System.err.println("Failed to replicate file due to an IO issue: " + e.getMessage());
            // Handle IO related errors here
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
            System.err.println("File replication was interrupted.");
            // Handle the interruption here
        } catch (Exception e) {
            // This catches any other exceptions that are not caught by the previous catch blocks
            System.err.println("An unexpected error occurred during file replication: " + e.getMessage());
            // Handle unexpected errors here
        }
    }

    /**
     * This method unlocks the given path. It unlocks the path and its parent directories.
     * It uses the same order as locking from top to bottom to avoid deadlocks.
     * @param path The path to unlock
     * @param exclusive True if the lock is exclusive, false otherwise
     */
    private void unlockFiles(Path path, boolean exclusive) {
        try {
            if(!path.isRoot()) {
                List<Path> pathsToUnlock = getSubPathsToLockUnlock(path);
                for(Path pathToUnlock : pathsToUnlock) {
                    ReadWriteLock rwLock = lockMap.get(pathToUnlock);
                    if (rwLock == null) {
                        throw new IllegalArgumentException("The file/directory is not locked.");
                    }
                    rwLock.unlock(false);
                }
            }
            ReadWriteLock rwLock = lockMap.get(path);
            if (rwLock == null) {
                throw new IllegalArgumentException("The file/directory is not locked.");
            }
            rwLock.unlock(exclusive);
        } catch (Exception e) {
            System.out.println("Error unlocking files: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Generates a list of subpaths for a given path to acquire/release lock.
     * This method is used to create a list of all subpaths that need to be locked or unlocked when a file operation is performed.
     * The list is sorted in ascending order to avoid deadlocks during concurrent operations.
     * @param path The path for which to generate the list of subpaths.
     * @return A list of Path objects representing all subpaths of the given path, sorted in ascending order.
     */
    private List<Path> getSubPathsToLockUnlock(Path path) {
        List<Path> pathsToLockUnlock = new ArrayList<>();
        String currentPath = "";
        for (String component : path.components) {
            // Build the path step by step, adding each component
            currentPath = currentPath + "/" + component;
            pathsToLockUnlock.add(new Path(currentPath));
        }
        pathsToLockUnlock.add(new Path("/"));
        pathsToLockUnlock.remove(path);
        System.out.println("Paths to lock before sorting: " + pathsToLockUnlock);
        Collections.sort(pathsToLockUnlock);
        System.out.println("Paths to lock after sorting: " + pathsToLockUnlock);
        return pathsToLockUnlock;
    }

    /**
     * Handles the HTTP request for the /read endpoint as per the REST API documentation.
     * This method is responsible for reading the contents of a file.
     * @param exchange The HttpExchange object encapsulating the HTTP request and response.
     * @throws IOException If an I/O error occurs.
     */
    private void sendResponse(HttpExchange exchange, int statusCode, Object responseBody) throws IOException {
        Gson gson = new Gson();
        if (responseBody != null) {
            // If there is a response body to send
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            String response = gson.toJson(responseBody);
            exchange.sendResponseHeaders(statusCode, response.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        } else {
            // If there's no response body, just send the headers
            exchange.sendResponseHeaders(statusCode, -1); // -1 indicates no content
        }
        exchange.close(); // Make sure to close the exchange
    }


    /**
     * Sends a command to a storage server.
     * @param method the method to send
     * @param port the port of the storage server
     * @param requestObj the request object
     * @return the HTTP response
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted
     */
    private HttpResponse<String> sendCommand(String method, int port,
                                               Object requestObj) throws IOException, InterruptedException {

        HttpResponse<String> response;
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + method))
                .setHeader("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(requestObj)))
                .build();

        response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }

    /**
     * Creates a file on a storage server on receiving a create_file request.
     * @param exchange the HTTP exchange
     * @param filePath the path of the file to create
     */
    private void createFileOnStorageServer(HttpExchange exchange, Path filePath) {
        try {
            if (registeredStorageServers.isEmpty()) {
                sendResponse(exchange, HttpURLConnection.HTTP_CONFLICT, new ExceptionReturn("IllegalStateException", "No storage servers are registered."));
            }

            // Convert the Set to an array to access a random server
            String[] serversArray = registeredStorageServers.toArray(new String[0]);
            int randomIndex = ThreadLocalRandom.current().nextInt(serversArray.length);
            String selectedServer = serversArray[randomIndex];

            // Assuming the server string format is "hostname:port"
            String[] serverParts = selectedServer.split(":");

            String serverHost = serverParts[0];
            int serverPort = Integer.parseInt(serverParts[2]);


            HttpResponse<String> response = sendCommand("/storage_create", serverPort, new PathRequest(filePath.toString()));
            boolean success = gson.fromJson(response.body(), BooleanReturn.class).success;
            if(success) {
                System.out.println("File "+ filePath +"created on storage server: " + selectedServer);
                pathToServerInfo.put(filePath, List.of(new ServerInfo(serverHost, Integer.parseInt(serverParts[1]))));
            } else {
                System.out.println("Error creating file on storage server: " + filePath);
            }
        } catch (Exception e) {
            System.out.println("Error creating file on storage server: " + e.getMessage());
        }
    }

}
