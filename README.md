# spanner-emulator-tester [![Build Status](https://travis-ci.org/olavloite/spanner-emulator-tester.svg?branch=master)](https://travis-ci.org/olavloite/spanner-emulator-tester)

Test project for Google Cloud Spanner Emulator.

This is a tester project for an emulator of Google Cloud Spanner, the horizontally scalable, globally consistent, relational database service from Google. When developing for Google Cloud Spanner, you normally have to do all your development work and testing against an actual Cloud Spanner instance instead of an emulator. This test project contains test cases that are run against a Google Cloud Spanner emulator that could be used for development and test purposes. This test project is intended to give an idea about what capabilities the emulator support.
In addition to the test cases in this project, the emulator is also used for all [integration tests](https://github.com/olavloite/spanner-jdbc/tree/master/src/test/java/nl/topicus/jdbc/test/integration) of the [open source JDBC driver](https://github.com/olavloite/spanner-jdbc) of Google Cloud Spanner.

The emulator is leightweight and can be run either on your local development machine or on a small cloud server. The main features of the emulatore are:
* It is intended for test and development purposes. That means that it does **not** in any way try to emulate the **scalability** of Google Cloud Spanner. It does however mean that it is **a lot faster** than Cloud Spanner when it comes to creating a small test database.
* The data that you write to the emulator are **persisted**. You could use the emulator for both automated testing as well as development.
* It is **leightweight**. You could run it on your local development machine or you could run it on a light cloud server.

The current version of the emulator runs as a separate process, but one of the planned features is allowing the emulator to be embedded and ran as an in-memory database.

Drop me an email at koloite@gmail.com if you are interested in a trial account for the emulator.
