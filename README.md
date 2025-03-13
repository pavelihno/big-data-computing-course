gradlew build
java --add-opens java.base/sun.nio.ch=ALL-UNNAMED -jar build/libs/BDC.jar 3 sentence_small.txt

java -jar build/libs/BDC.jar 3 sentence_small.txt


gradlew compileJava
java -cp build/classes/java/main WordCountExample 1 sentence_small.txt
