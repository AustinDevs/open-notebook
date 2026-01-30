# Database - SurrealDB Configuration

Open Notebook uses SurrealDB for its database needs. 

---

## Default Configuration

Open Notebook should work out of the box with SurrealDB as long as the environment variables are correctly setup. 


### DB running in the same docker compose as Open Notebook (recommended)

The example above is for when you are running SurrealDB as a separate docker container, which is the method described [here](../1-INSTALLATION/docker-compose.md) (and our recommended method). 

```env
SURREAL_URL="ws://surrealdb:8000/rpc"
SURREAL_USER="root"
SURREAL_PASSWORD="root"
SURREAL_NAMESPACE="open_notebook"
SURREAL_DATABASE="open_notebook"
```

### DB running in the host machine and Open Notebook running in Docker

If ON is running in docker and SurrealDB is on your host machine, you need to point to it. 

```env
SURREAL_URL="ws://your-machine-ip:8000/rpc" #or host.docker.internal
SURREAL_USER="root"
SURREAL_PASSWORD="root"
SURREAL_NAMESPACE="open_notebook"
SURREAL_DATABASE="open_notebook"
```

### Open Notebook and Surreal are running on the same machine

If you are running both services locally or if you are using the deprecated [single container setup](../1-INSTALLATION/single-container.md)

```env
SURREAL_URL="ws://localhost:8000/rpc"
SURREAL_USER="root"
SURREAL_PASSWORD="root"
SURREAL_NAMESPACE="open_notebook"
SURREAL_DATABASE="open_notebook"
```

## Multiple databases

You can have multiple namespaces in one SurrealDB instance and you can also have multiple databases in one instance. So, if you want to setup multiple open notebook deployments for different users, you don't need to deploy multiple databases.

### JWT-Based Multi-Tenancy (Recommended for External Integration)

If you're embedding Open Notebook in another application (e.g., Laravel/Filament via iframe), you can use JWT authentication to automatically route users to their own namespace:

1. Enable JWT auth in your environment:
   ```env
   JWT_AUTH_ENABLED=true
   JWT_SECRET=your-strong-secret-key
   ```

2. Generate a JWT in your parent application with the user's namespace:
   ```php
   // Laravel example
   $token = JWT::encode([
       'namespace' => 'user_' . $user->id,
       'database' => 'open_notebook',
       'sub' => (string) $user->id,
       'exp' => time() + 3600  // 1 hour expiry
   ], config('app.jwt_secret'), 'HS256');
   ```

3. Pass the token when loading Open Notebook:
   ```html
   <iframe src="https://open-notebook.example.com?token={{ $token }}"></iframe>
   ```

The API will automatically use the namespace from the JWT for all database operations, providing complete data isolation between users.
