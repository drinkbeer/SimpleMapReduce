# SimpleMapReduce
A simple version of MapReduce written in Go.

# Sequential Tests
`go test '-run=TestSequential*' /Users/jchome/src/github.com/drinkbeer/src/SimpleMapReduce/src/mapreduce`

# Parallel Tests
`go test '-run=TestParallel*' /Users/jchome/src/github.com/drinkbeer/src/SimpleMapReduce/src/mapreduce`

# All Tests
`go test /Users/jchome/src/github.com/drinkbeer/src/SimpleMapReduce/src/mapreduce`

# TODO
- [ ] Make this a multiple cluster version of MapReduce library, and can be deployed in Kubernetes cluster.
- [ ] Can submit ad-hoc workload to the cluster.

