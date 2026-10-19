# minikv

Companion code for part 1 of "Write your own deterministic simulation test in Go".

The post explains the design. This module is the program those snippets are taken from: a sans-I/O key-value server, a simulator, and a test that runs one seed twice.

```
go test
go test -v -run TestDeterminism
```

Seed 1 prints trace hash `c43a5572d697e036`, in 16 steps and 27 draws from the random stream.
