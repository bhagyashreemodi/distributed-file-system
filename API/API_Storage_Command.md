# Storage Server API Specification - Command Interface

The naming server uses this interface to communicate commands to a storage server. This
interface will be created using the localhost/127.0.0.1 server address and the port number
included in the `storageNCommand` strings defined in `test/ServerCommands.java`.

If the storage server cannot parse a received command, it should respond with `400 Bad Request`.

------

## `/storage_create` Command

**Description**: Naming server uses this command to instruct a storage server to create a file
in its local storage directory using the given path.

### Request from naming server

**Command**: `/storage_create`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: The path string to the file to be created. The parent directory(s) will be created
if if does not exist. The path cannot be the root directory.

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Response to naming server

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the file was created (`true`) or not (`false`).

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to naming server

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "IllegalArgumentException",
    "exception_info": "Path is invalid"
}
```

* *exception_type*: `IllegalArgumentException` if the path is invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/storage_delete` Command

**Description**: Naming server uses this command to instruct a storage server to delete a file
or directory from its local storage. If the file is a directory and cannot be deleted, some, all,
or none of its contents may be deleted by this operation.

### Request from naming server

**Command**: `/storage_delete`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: The path string to the file/directory to be deleted. The root directory cannot be deleted.

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Response to naming server

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the file was deleted (`true`) or not (`false`).

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to naming server

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "IllegalArgumentException",
    "exception_info": "Path is invalid"
}
```

* *exception_type*: `IllegalArgumentException` if the path is invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/storage_copy` Command

**Description**: Naming server uses this command to instruct a storage server to fetch a file 
from another storage server and copy it to its local storage.

### Request from naming server

**Command**: `/storage_copy`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file",
    "server_ip": "localhost",
    "server_port": 1111
}
```

* *path*: The path string of the file to be copied
* *server_ip*: IP address of the storage server hosting the file
* *server_port*: storage port of the storage server hosting the file

A sample Java class representing this command can be found at `common/CopyRequest.java`.

### Response to naming server

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the file was successfully copied (`true`) or not (`false`).

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to naming server

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "File not found on storage server"
}
```

* *exception_type*: 
    * `FileNotFoundException` if the peer storage server does not have the file or if the path refers to a directory
    * `IllegalArgumentException` if the path is invalid
    * `IOException` if an I/O exception occurs while communicating with the peer storage server
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

