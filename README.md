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

    - Via PuTTY (Windows)

        Hostname: `<account_name>@ssh.studenti.math.unipd.it`

        Port: `22`

        Password: `<account_password>`

    - Via SSH (Linux/Mac)

        ```sh
        ssh -p 22 <account_name>@ssh.studenti.math.unipd.it
        ```

2. Connect to cloud

    - Via PuTTY (Windows)

        Hostname: `group23@147.162.226.106`

        Port: `2222`

        Password: `group23password`
    
    - Via SSH (Linux/Mac)

        ```sh
        ssh -p 2222 group23@147.162.226.106
        ```

#### Run the Application

**On Local Machine:**
```sh
gradlew run -PmainClass=G23GEN --args="1000 4"
gradlew run -PmainClass=G23HW2 --args="./data/generated_points.csv 5 4 30"
gradlew run -PmainClass=G23Visualize --args="./data/generated_points.csv"
gradlew run -PmainClass=G23Visualize --args="./data/standard_points.csv"
gradlew run -PmainClass=G23Visualize --args="./data/fair_points.csv"
```

**On Cloud:**

0. Make sure you're using cloud implementation (`LOCAL = false`)

1. Build the project using the Gradle wrapper

    ```sh
    gradlew -PbuildType=cloud -PmainClass=G23HW2 clean jar
    ```

2. Transfer the JAR file to the cloud

    - Via PSCP (Windows) to the UNIPD machine

        ```sh
        pscp -P 22 build/libs/BDC.jar <account_name>@ssh.studenti.math.unipd.it:/home/0/2024/<account_name>/
        ```
    
    - Via PSCP (Windows) to the cloud

        ```sh
        pscp -P 2222 build/libs/BDC.jar group23@147.162.226.106:/home/group23/
        ```

    - Via SCP (Linux/Mac)

        ```sh
        scp -P 2222 build/libs/BDC.jar group23@147.162.226.106:/home/group23/
        ```

3. Run the job on the cloud

    *Number of executors*: 2, 4, 8, 16

    *HDFS file paths*: 
    - `/data/BDC2425/artificial1M7D100K.txt`
    - `/data/BDC2425/artificial4M7D100K.txt`

    ```sh
    cd /home/group23
    spark-submit --num-executors <num_executors> --class G23HW2 BDC.jar <hdfs_file_path> 16 100 10
    ```