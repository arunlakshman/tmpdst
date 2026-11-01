// Package runsettings reads the run settings of a checker comparison from
// environment variables. It turns them into a checker.Options value, so
// every test package uses the same variable names and the same rules.
//
// This is an edge package. It may use os, time and testing. The package
// checker itself must not use them, so the reading of the environment
// lives here.
//
// Variables, from the strongest to the weakest:
//
//   - MDS_SEED=<n>: run only seed n, where n is 0 or more.
//   - MDS_CAMPAIGN=<duration>: run seeds 1, 2, 3 and so on until the
//     duration ends, for example 30s or 5m.
//   - MDS_SEEDS=<n>: run seeds 1 to n, where n is 1 or more.
//
// A bad value fails the test at once. The package never ignores a bad value.
//
// Terms:
//
//   - seed: a number that fixes the random operations, so a run can repeat.
//   - campaign: a long run that is limited by time and not by a seed count.
//   - replay: run one failing seed again with the same operations.
package runsettings
