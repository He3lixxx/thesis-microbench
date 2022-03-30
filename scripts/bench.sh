#! /usr/bin/env bash
thread_count=1
memory_size="2g"
warmup=10
runtime=60

set -x

./bench -t$thread_count -m$memory_size -pnative -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pflatbuf -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pprotobuf -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pavro -w$warmup -i$runtime

./bench -t$thread_count -m$memory_size -pcsvstd -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pcsvfastfloat -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pcsvfastfloatcustom -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -pcsvbenstrasser -w$warmup -i$runtime

./bench -t$thread_count -m$memory_size -prapidjson -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -prapidjsoninsitu -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -prapidjsonsax -w$warmup -i$runtime

./bench -t$thread_count -m$memory_size -psimdjson -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -psimdjsonec -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -psimdjsonece -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -psimdjsonu -w$warmup -i$runtime
./bench -t$thread_count -m$memory_size -psimdjsonooo -w$warmup -i$runtime
