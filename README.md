# wmillops
Windmill Base Architechture For Setup, Storage,... 


## Preliminaries
First, we need to rebuild `windmill` image  in `Dockerfile.worker` duo to limited feature.

The `Dockerfile.worker` defines the custom Docker image for the Windmill worker. It starts from a base image, installs necessary dependencies, and sets up the environment for `playwright`.

### Building the Custom Worker Image
To build the custom worker image, run the following command in the terminal:

```
docker build -t <dockerhub_username>/windmill-worker:latest -f Dockerfile.worker .
```

### Pushing the Image to Docker Hub
After building the image, you can push it to your Docker Hub repository with the following command:

```
docker push <dockerhub_username>/windmill-worker:latest
```

## Running the Application
Create a file `.env` and put this below line into it.

```yaml
WM_IMAGE=<dockerhub_username>/windmill-worker:latest
```

Then, you can start the application with:

```
docker-compose up
```