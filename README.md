## Requirements

- Java 17.0.6 LTS (JDK 17)

## Build the project using the Gradle wrapper
```sh
gradlew build
```

## Run the application with the Gradle wrapper using provided arguments

### Machine Setup
```sh
gradlew run -PmainClass=WordCountExample --args="3 ./data/sentence_small.txt"
```

### Homework 1
```sh
gradlew run -PmainClass=G23HW1 --args="./data/uber_small.csv 1 4 20"
gradlew run -PmainClass=G23HW1 --args="./data/uber_small.csv 2 4 20"
```


### Homework 2

#### Connect to cloud

1. Connect to UNIPD machine via SSH (if not in UNIPD network)

    1. via PuTTY (Windows)

        hostname: `<account_name>@ssh.studenti.math.unipd.it`

        port: `22`

        password: `<account_password>`

    2. via SSH (Linux/Mac)

        ```sh
        ssh -p 22 <account_name>@ssh.studenti.math.unipd.it
        ```

2. Connect to cloud

    1. via PuTTY (Windows)

        hostname: `group23@147.162.226.106`

        port: `2222`

        password: `group23password`
    
    2. via SSH (Linux/Mac)

        ```sh
        ssh -p 2222 groupXX@147.162.226.106
        ```

#### Run the application

*on local machine:*
```sh
gradlew run -PmainClass=G23GEN --args="1000 4"
gradlew run -PmainClass=G23Visualize --args="./data/generated_points.csv"
gradlew run -PmainClass=G23HW2 --args="./data/uber_small.csv 5 4 20"
```


*on cloud:*
```sh

```