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
```sh
gradlew run -PmainClass=G23GEN --args="1000 4"
gradlew run -PmainClass=G23Visualize --args="./data/generated_points.csv"
```