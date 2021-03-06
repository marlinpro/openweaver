# Intentions
To provide consistent build environment across dev machines for OpenWeaver (in Google's terms - [hermetic builds](https://sre.google/sre-book/release-engineering/)). This is done by providing the compilation toolchain under a containerized environment so that host environment toolchains do not affect the builds.

# Prerequisistes
Install docker using your package manager. For example in arch linux (using paru AUR helper):
```
paru -Sy docker
```
At the time of writing this, the following versions were found:
- containerd 1.5.2
- runc 1.0.0 rc95
- docker 20.10.7

Start the docker daemon using:
```
sudo systemctl start docker.service
```

## Build and run the container
Build the container (with pwd of shell at `Dockerfile`'s directory) using
```
sudo docker build --build-arg uid=$(echo $UID) --build-arg uname=$(whoami) -t ow_builder .
```
The build procedure will provide you with versions of components in one of the steps while building the container image. Verify correctness (good output should look close to the following):
```
BUILD ENVIRONMENT STATUS
Component                        Expected                     Actuals
---------                        --------                     -------
 |- doxygen version              1.8.17                       1.8.17
 |- make version                 GNU Make 4.2.1               GNU Make 4.2.1
 |- cmake version                3.16.3                       cmake version 3.16.3
 |- autoconf version             2.69                         autoconf (GNU Autoconf) 2.69
 |- glibcxx highest version      GLIBCXX_3.4.28               GLIBCXX_3.4.28
 |- c++ --version                clang 10.0.0                 clang version 10.0.0-4ubuntu1 
 |- cc --version                 clang 10.0.0                 clang version 10.0.0-4ubuntu1 
 |- ldd --version                2.31                         ldd (Ubuntu GLIBC 2.31-0ubuntu9.2) 2.31
```
Remove intermediate build images using
```
sudo docker image prune -f
```
Export your Openweaver directory using:
```
export OWDIR=/home/supragya/Projects/OpenWeaver
```
Run the container using:
```
sudo docker run \
    --privileged \
    --interactive \
    --tty \
    --entrypoint /bin/bash \
    --volume `echo $OWDIR`:/OpenWeaver \
    --workdir /OpenWeaver \
    --user `whoami` \
    ow_builder:latest
```
This will mount OpenWeaver onto container and start an interactive shell on the container with working directory set to OpenWeaver's dir.

**TIP**: alias the above command to some easy command with OWDIR set to a constant value to invoke the container on the go while dev.

The build commands such as 
```
mkdir build && cd build
cmake .. & make -j8
```
should work now

**TIP**: The container has a distinct shell prompt of the following form to easily distinguish it from the host machine
```
[OW-BUILDER] user:root pwd:/OpenWeaver $ ...
```