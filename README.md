# DAS-839 NoSQL - Hadoop Part 1

This project was created as part of the **DAS-839 NoSQL** course. It sets up a Hadoop environment using Docker for running Hadoop in a pseudo-distributed mode.

## Getting Started

Follow these steps to pull and run the Hadoop Docker image.

### 1. Pull the Docker Image

```bash
docker pull mightyshashank/hadoopplayground-images:v3
```

### 2. Run the Docker Container

```bash
docker run -it --name hadoop-container -p 9870:9870 -p 8088:8088 --network bridge mightyshashank/hadoopplayground-images:v3
```

- `--name hadoop-container`: Names the running container.
- `-p 9870:9870`: Maps the Hadoop NameNode web UI to port 9870.
- `-p 8088:8088`: Maps the ResourceManager web UI to port 8088.
- `--network bridge`: Uses Docker's default bridge network.

### 3. Access the Container's Shell

Once the container is running, access the shell by executing:

```bash
docker exec -it hadoop-container /bin/bash
```

## Hadoop Web Interfaces

- **NameNode UI**: [http://localhost:9870](http://localhost:9870)
- **ResourceManager UI**: [http://localhost:8088](http://localhost:8088)

## Notes

- Ensure Docker is installed and running on your system.
- You may need to adjust firewall rules if ports 9870 and 8088 are already in use.

## License

This project is part of the DAS-839 NoSQL course materials.
