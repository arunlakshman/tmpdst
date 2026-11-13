// Package paritygen makes the parity files of the fake services
// (../testdata/parity_*.txt) from the OLD fakes.
//
// The generator is a test with the build tag "paritygen" (gen_test.go). It runs the
// cases of servicetest against the old fakes of internal/verify/simulator, which
// run inside a dst kernel world through a small adapter, and writes their
// transcripts. The new fakes are compared with these files by parity_test.go in
// the parent directory.
//
// To make the files again, while the old tree exists:
//
//	go test -tags paritygen ./internal/verify/simulation/fakeservices/paritygen
//
// Plan task T1.12d deletes the old tree. It must delete this directory too: it
// cannot be built without the old fakes. The parity files stay.
package paritygen
