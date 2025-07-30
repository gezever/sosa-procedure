# sosa-procedure
spark vs. inference

## Run examples with JDK 17
Open module settings and select JDK 17 as module SDK
Run and stop application.
Go to Edit Configurations and Add VM options:
--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED