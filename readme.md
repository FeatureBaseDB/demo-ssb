# import data
see https://github.com/pilosa/pdk


# run demo app
`git clone https://github.com/pilosa/demo-ssb.git`

`cd demo-ssb`

`go build *.go && ./main -p node0.your.pilosa.cluster:10101 -i ssb`

`curl localhost:8000/query/1.1` 
OR
`./run_benchmarks.sh`
