#  Jupyter Notebook Docker Environment

This repository sets up a reproducible Jupyter Notebook environment using Docker, complete with autocompletion and enhanced features. It mounts shared folders for persistent storage and is optimized for remote access via SSH tunneling.

---

##  Components

- `Dockerfile`: Defines the base environment and dependencies
- `startup.sh`: Configures additional features like Jupyter autocompletion
- `build_docker.sh`: Builds the Docker image
- `run_docker.sh`: Launches the container with mounted volumes

---

##  Shared Volumes

The container mounts the following host directories to persist your work:

```bash
-v /home/jovyan/work:/home/jovyan/work
-v /home/sky/Playground:/home/jovyan/playground
```

##  Remote Access via SSH Tunne

```bash
ssh -L 8888:localhost:8888 your-user@your-server-ip
```

Then open broswer and visit:
```bash
http://127.0.0.1:8888/lab?token=2c965af27b27569ca5fec3f940c2ba66b11d87dd11f82318
```
*Note the token should use your own token display in the console or docker logs*

##  Cleanup
The container is ephemeral and will be removed after shutdown. Your data remains safe in the mounted volumes.

