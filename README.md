# reroft

[![CircleCI](https://circleci.com/gh/rerorero/reroft.svg?style=svg)](https://circleci.com/gh/rerorero/reroft)
[![codecov](https://codecov.io/gh/rerorero/reroft/branch/master/graph/badge.svg)](https://codecov.io/gh/rerorero/reroft)

Implementation of [Raft Consensus Algorithm](https://raft.github.io/raft.pdf) for Scala.

## Build Your State Machine
You can build your application as a state machine on top of `RaftActor`. For more details, please [see the example of arithmetic operation](app_calc/src/main/scala/com/github/rerorero/reroft/calc/StateMachine.scala).

## TODO
- [x] leader election
- [x] log replication
- [ ] configuration change support
- [ ] log replication

## LICENSE
This project is licensed under the MIT License.

