# Naming Server API Specification - Registration Interface

Each storage server uses this API only once at startup time. This interface will be created 
using the localhost/127.0.0.1 server address and the port number included in the `namingCommand` 
string defined in `test/ServerCommands.java`.

If the naming server cannot parse a received command, it should respond with `400 Bad Request`.

------

## `/register` Command

**Description**: This command is used by a new storage server to register itself with 
the naming server. The storage server includes its own address and service port numbers,
along with a list of paths of the files that it is hosting. If desired, the naming server
will add these files to its file system tree; otherwise, the naming server can instruct
the storage server to delete a subset of the reported files. After the storage server
deletes whatever files it is instructed to delete, it must prune its directory tree by
removing all directories under which no files are stored.

Registration requires the naming server to lock the root directory for exclusive access,
so it is best done when there is not heavy usage of the file system.

### Request from storage server to naming server

**Command**: `/register`

**Method**: `POST`

**Input Data**:
```json
{
    "storage_ip": "localhost",
    "client_port": 1111,
    "command_port": 2222,
    "files": [
        "/fileA",
        "/path/to/fileA",
        "/path/to/fileB",
        "/path/to/another/fileA"
    ]
}
```

* *storage_ip*: storage server's IP address
* *client_port*: storage server's listening port for client requests
* *command_port*: storage server's listening port for naming server commands
* *files*: list of paths of files stored on the storage server

A sample Java class representing this command can be found at `common/RegisterRequest.java`.


### Successful response from naming server to storage server

**Code**: `200 OK`

**Content**:
```json
{
    "files": [
        "/path/to/fileA",
        "/fileA"
    ]
}
```

* *files*: list of paths of files that the storage server should delete from its local storage

A sample Java class representing this response can be found at `common/FilesReturn.java`.


### Error response from naming server -- storage server already registered

**Code**: `409 Conflict`

**Content**:
```json
{
    "exception_type": "IllegalStateException",
    "exception_info": "This storage server is already registered."
}
```

* *exception_type*: `IllegalStateException` is used to indicate that this operation is not allowed.
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

