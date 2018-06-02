# spanner-emulator-tester

Test project for the Google Cloud Spanner Emulator.

The emulator currently has a number of known issues:
* Column names are always returned in upper case in result sets
* Column names that must be quoted are case sensitive
