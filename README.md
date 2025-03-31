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
gradlew run -PmainClass=G23HW1 --args="./data/uber_small.csv 3 4 5"
```