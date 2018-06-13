# spanner-emulator-tester [![Build Status](https://travis-ci.org/olavloite/spanner-emulator-tester.svg?branch=master)](https://travis-ci.org/olavloite/spanner-emulator-tester)

Test project for Google Cloud Spanner Emulator.
Send me an email at koloite@gmail.com if you are interested in a trial account or license for the emulator.

## Try it out
You could also try the emulator out by writing a small test case and submitting it as a pull request to this project. Have a look at the test class for a simple example that you could use as a template for your own test case: https://github.com/olavloite/spanner-emulator-tester/blob/master/src/test/java/io/github/olavloite/spanner/emulator/tryitout/ExampleTryItOutTest.java . Submit your test case as a pull request and it will automatically be built and tested against an emulator running on a small server.

## Description
This is a tester project for an emulator of Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. When developing for Google Cloud Spanner, you normally have to do all your development work and testing against an actual Cloud Spanner instance instead of an emulator. This test project contains test cases that are run against a Google Cloud Spanner emulator that could be used for development and test purposes. This test project is intended to show the capabilities the emulator support.
In addition to the test cases in this project, the emulator is also used for all [integration tests](https://github.com/olavloite/spanner-jdbc/tree/master/src/test/java/nl/topicus/jdbc/test/integration) of the [open source JDBC driver](https://github.com/olavloite/spanner-jdbc) of Google Cloud Spanner.

The emulator is lightweight and can be run either on your local development machine or on a small cloud server. The main features of the emulatore are:
* It is intended for test and development purposes. That means that it does **not** in any way try to emulate the **scalability** of Google Cloud Spanner. It does however mean that it is **a lot faster** than Cloud Spanner when it comes to creating a small test database.
* The data that you write to the emulator are **persisted**. You could use the emulator for both automated testing as well as development.
* It is **lightweight**. You could run it on your local development machine or you could run it on a light (cloud) server.

The current version of the emulator runs as a separate process, but one of the planned features is allowing the emulator to be embedded and run as an in-memory database.

## How does it work?
The emulator implements the Spanner gRPC interface, which means that you can use the emulator with the programming language of your own choice. All mutations that are sent to the emulator are stored in a standard relational database, and queries are executed against this relational database. Any translations that are needed between the Spanner dialact and the dialect of the backing database are handled by the emulator, as are all session- and transaction management.
The emulator is intended for development and automated tests. It tries to emulate the entire API of Cloud Spanner, but it does not in any way try to emulate the scalability of Cloud Spanner. It does however offer a lot better performance for generating and filling test- or development databases than creating a real Cloud Spanner instance and database.
